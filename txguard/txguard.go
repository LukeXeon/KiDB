// Package txguard 是写入路径的编排层（docs/05）：
// 预读旧行/旧回执 → 展开索引撤销/重建描述符 → 唯一预约（SET NX 两阶段）
// → write_row.lua 单 slot 原子提交 → 释放被替换的旧预约。
// stale（版本冲突）整体重试 ≤3 次。
package txguard

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"kidb"
	"kidb/keycodec"
	"kidb/meta"
	"kidb/script"
)

// maxStaleRetries 是 BucketMap/行版本冲突的整体重试上限（docs/05 §5.5）。
const maxStaleRetries = 3

// Guard 编排单行写入。通过 kidb.Client 下发命令，满足契约 R1~R7。
type Guard struct {
	cli   kidb.Client
	reg   *script.Registry
	clock func() time.Time // 测试可注入
}

// New 构造 Guard。
func New(cli kidb.Client, reg *script.Registry) *Guard {
	return &Guard{cli: cli, reg: reg, clock: time.Now}
}

// SetClock 注入测试时钟。
func (g *Guard) SetClock(c func() time.Time) { g.clock = c }

// WriteReq 是一次单行写入请求。Fields 为编码后的字段值
// （编码规则见 docs/03 §3.4；不得包含 `_` 前缀保留列与主键列——主键经 PK 传入）。
type WriteReq struct {
	Table          *meta.TableDef
	PK             string
	Fields         map[string]string
	TTL            time.Duration // 0 = 无 TTL；<0 = 软删除（立即过期走清扫，docs/07 §7.1）
	ExpectedOldVer *int64        // nil = 不校验；非 nil = CAS 写语义（docs/05 §5.6）
}

// Result 是写入结果。
type Result struct {
	OldVer       uint64
	NewVer       uint64
	StaleRetries int
}

// indexOp 是单个索引的撤销/重建描述符（对应 Lua ARGV 协议）。
type indexOp struct {
	undoKey    string
	undoMember string
	redoKey    string
	redoMember string
	redoScore  float64
	hasRedo    bool
}

// WriteRow 执行单行写入（INSERT/UPDATE/UPSERT 共用）。
func (g *Guard) WriteRow(ctx context.Context, req WriteReq) (Result, error) {
	if req.Table == nil || req.PK == "" {
		return Result{}, fmt.Errorf("txguard: table/pk required")
	}
	for f := range req.Fields {
		if err := meta.ValidateReserved(f); err != nil {
			return Result{}, fmt.Errorf("%w: %v", kidb.ErrContractViolation, err)
		}
		if strings.EqualFold(f, req.Table.PK) {
			return Result{}, fmt.Errorf("%w: 主键列 %q 经 PK 传入，不在 Fields", kidb.ErrContractViolation, f)
		}
	}
	rowkey := keycodec.RowKey(req.Table.Name, req.PK)
	slot := keycodec.Slot(rowkey)

	var acquired []string // 本事务占有的唯一预约 key（回滚/重试用）
	var res Result
	for attempt := 0; attempt < maxStaleRetries; attempt++ {
		res.StaleRetries = attempt
		stale, err := g.writeAttempt(ctx, req, rowkey, slot, &acquired, &res)
		if err != nil {
			g.rollbackReservations(ctx, acquired)
			return Result{}, err
		}
		if !stale {
			return res, nil
		}
		// stale：行被并发改写，整体重读重试（预约 key 我方已持有，重试幂等）
	}
	g.rollbackReservations(ctx, acquired)
	return Result{}, fmt.Errorf("%w: write %s after %d attempts", kidb.ErrStaleMetadata, rowkey, maxStaleRetries)
}

// writeAttempt 完成一轮"预读 → 展开 → 预约 → Lua 提交"；stale=true 表示版本冲突。
func (g *Guard) writeAttempt(ctx context.Context, req WriteReq, rowkey string, slot uint16,
	acquired *[]string, res *Result) (stale bool, err error) {

	t := req.Table
	rcptkey := keycodec.ReceiptKey(t.Name, req.PK)

	// 预读旧行；旧行空则预读回执（主键复活：撤销条目按回执展开，docs/05 §5.1 第 2 步）
	oldRow, err := g.hgetall(ctx, rowkey)
	if err != nil {
		return false, err
	}
	var oldRcpt map[string]string
	if len(oldRow) == 0 {
		oldRcpt, err = g.hgetall(ctx, rcptkey)
		if err != nil {
			return false, err
		}
	}

	ops := buildIndexOps(t, slot, req.PK, oldRow, req.Fields)
	// 复活路径：把旧回执的索引条目并入撤销集
	if len(oldRow) == 0 && len(oldRcpt) > 0 {
		ops = mergeReceiptUndo(ops, oldRcpt)
	}

	// 唯一预约（docs/05 §5.3：按值散列 SET NX，占有者活检查 + 自愈）
	var uniqEntries [][2]string // (indexID, reservationKey) 记入回执
	now := g.clock()
	for _, idx := range t.Indexes {
		if idx.Kind != meta.IndexUnique {
			continue
		}
		newVal, ok := req.Fields[idx.Columns[0]]
		if !ok {
			continue
		}
		rkey := keycodec.UniqueKey(t.Name, idx.ID, newVal)
		ok2, err := g.reserveUnique(ctx, rkey, rowkey, now)
		if err != nil {
			return false, err
		}
		if !ok2 {
			return false, fmt.Errorf("%w: %s on %s", kidb.ErrDuplicateKey, newVal, idx.ID)
		}
		if !contains(acquired, rkey) {
			*acquired = append(*acquired, rkey)
		}
		uniqEntries = append(uniqEntries, [2]string{idx.ID, rkey})
	}

	// 组装 KEYS / ARGV
	bucketKeys, argvTail := assembleIndexArgs(ops)
	keys := make([]string, 0, 4+len(bucketKeys))
	keys = append(keys, rowkey)
	keys = append(keys, bucketKeys...)
	expShards := t.EffectiveExpShards()
	keys = append(keys,
		keycodec.ExpKeyN(t.Name, slot, keycodec.ExpShardFor(req.PK, expShards), expShards),
		keycodec.CntKey(t.Name, slot),
		rcptkey,
	)

	// _ver 语义（docs/05 §5.6）：
	//  - ARGV[5] 恒为本次预读观察到的 old_ver——Lua 内再读不符说明预读→提交间
	//    有并发写入，stale 让网关以新旧行重展开整体重试（"按当前 old 重放合并"）；
	//  - 调用方 ExpectedOldVer 非 nil 是 CAS 写语义：预读版本不符即 fail-fast，
	//    不重试（重试不会改变调用方的过期预期）。
	observed := int64(oldVerOf(oldRow))
	if req.ExpectedOldVer != nil && *req.ExpectedOldVer != observed {
		return false, fmt.Errorf("%w: %s 期望 _ver=%d 当前=%d",
			kidb.ErrStaleMetadata, rowkey, *req.ExpectedOldVer, observed)
	}
	ttlms := int64(req.TTL / time.Millisecond)
	if req.TTL < 0 {
		ttlms = 1 // 软删除：立即过期走清扫
	}
	argv := []any{
		"W", req.PK, strconv.FormatInt(ttlms, 10), strconv.FormatInt(now.Unix(), 10),
		strconv.FormatInt(observed, 10), strconv.Itoa(len(ops)),
	}
	argv = append(argv, argvTail...)
	argv = append(argv, strconv.Itoa(len(req.Fields)))
	for f, v := range req.Fields {
		argv = append(argv, f, v)
	}
	argv = append(argv, strconv.Itoa(len(uniqEntries)))
	for _, e := range uniqEntries {
		argv = append(argv, e[0], e[1])
	}

	wr, ok := g.reg.Get("write_row")
	if !ok {
		return false, fmt.Errorf("txguard: write_row.lua not registered")
	}
	out, err := g.cli.Eval(ctx, wr, keys, argv...)
	if err != nil {
		return false, err
	}
	arr, ok := out.([]any)
	if !ok || len(arr) == 0 {
		return false, fmt.Errorf("txguard: unexpected write_row reply %T %v", out, out)
	}
	switch fmt.Sprint(arr[0]) {
	case "stale":
		return true, nil
	case "ok":
		res.OldVer = parseUint64(fmt.Sprint(arr[1]))
		if len(arr) > 2 {
			res.NewVer = parseUint64(fmt.Sprint(arr[2]))
		}
	default:
		return false, fmt.Errorf("txguard: write_row unknown status %v", arr[0])
	}

	// 提交成功：释放被替换值的旧唯一预约（异 slot DEL；失败残留由预约侧自愈兜底）
	for _, idx := range t.Indexes {
		if idx.Kind != meta.IndexUnique {
			continue
		}
		col := idx.Columns[0]
		oldVal, had := oldRow[col]
		newVal, has := req.Fields[col]
		if had && (!has || oldVal != newVal) {
			_ = g.releaseUnique(ctx, keycodec.UniqueKey(t.Name, idx.ID, oldVal), rowkey)
		}
	}
	// 复活路径追加：旧行已过期时旧唯一值不在 oldRow，按旧回执的 __uniq 登记释放
	// （跳过本次新占有的预约——同值复约场景防误删）。
	if len(oldRow) == 0 {
		for f, v := range oldRcpt {
			if strings.HasPrefix(f, "__uniq:") && !contains(acquired, v) {
				_ = g.releaseUnique(ctx, v, rowkey)
			}
		}
	}
	return false, nil
}

// DeleteRow 删除单行（命中已过期行 = 0 rows affected，docs/05 §5.5）。
func (g *Guard) DeleteRow(ctx context.Context, t *meta.TableDef, pk string) (deleted bool, err error) {
	rowkey := keycodec.RowKey(t.Name, pk)
	slot := keycodec.Slot(rowkey)
	for attempt := 0; attempt < maxStaleRetries; attempt++ {
		oldRow, err := g.hgetall(ctx, rowkey)
		if err != nil {
			return false, err
		}
		ops := buildIndexOps(t, slot, pk, oldRow, nil)
		bucketKeys, argvTail := assembleIndexArgs(ops)
		keys := append([]string{rowkey}, bucketKeys...)
		expShards := t.EffectiveExpShards()
		keys = append(keys,
			keycodec.ExpKeyN(t.Name, slot, keycodec.ExpShardFor(pk, expShards), expShards),
			keycodec.CntKey(t.Name, slot),
			keycodec.ReceiptKey(t.Name, pk),
		)
		argv := []any{
			"D", pk, "0", strconv.FormatInt(g.clock().Unix(), 10),
			strconv.FormatUint(oldVerOf(oldRow), 10), strconv.Itoa(len(ops)),
		}
		argv = append(argv, argvTail...)

		wr, _ := g.reg.Get("write_row")
		out, err := g.cli.Eval(ctx, wr, keys, argv...)
		if err != nil {
			return false, err
		}
		arr, _ := out.([]any)
		if len(arr) > 0 && fmt.Sprint(arr[0]) == "stale" {
			continue
		}
		// 提交成功：释放该行的唯一预约（异 slot DEL）
		if len(oldRow) > 0 {
			for _, idx := range t.Indexes {
				if idx.Kind != meta.IndexUnique {
					continue
				}
				if v, ok := oldRow[idx.Columns[0]]; ok {
					_ = g.releaseUnique(ctx, keycodec.UniqueKey(t.Name, idx.ID, v), rowkey)
				}
			}
			return true, nil
		}
		return false, nil
	}
	return false, fmt.Errorf("%w: delete %s after %d attempts", kidb.ErrStaleMetadata, rowkey, maxStaleRetries)
}

// NextAutoID 取 AUTO_INCREMENT 序列值（docs/05 §5.4：
// seq:{table} 与行异 slot，必须先于写入 Lua 单独 INCR；空洞语义与 MySQL 一致）。
func (g *Guard) NextAutoID(ctx context.Context, table string) (uint64, error) {
	res, err := g.cli.Do(ctx, "INCR", keycodec.SeqKey(table))
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(fmt.Sprint(res), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("txguard: seq %s: %v", table, res)
	}
	return n, nil
}

// reserveUnique 执行唯一预约两阶段（docs/05 §5.3）：
// SET NX → 失败则读占有者 → EXISTS 判活：活=false 返回；死=自愈重占。
// 占有者就是本行（重试场景）视为成功。
func (g *Guard) reserveUnique(ctx context.Context, rkey, rowkey string, now time.Time) (bool, error) {
	owner := rowkey + "|" + strconv.FormatInt(now.Unix(), 10)
	res, err := g.cli.Do(ctx, "SET", rkey, owner, "NX")
	if err != nil {
		return false, err
	}
	if res != nil && fmt.Sprint(res) == "OK" {
		return true, nil
	}
	// NX 失败：读占有者
	cur, err := g.cli.Do(ctx, "GET", rkey)
	if err != nil {
		return false, err
	}
	if cur == nil {
		return true, g.retryReserve(ctx, rkey, owner) // 刚好被释放
	}
	ownerRow := strings.SplitN(fmt.Sprint(cur), "|", 2)[0]
	if ownerRow == rowkey {
		return true, nil // 我方已持有（stale 重试）
	}
	exists, err := g.cli.Do(ctx, "EXISTS", ownerRow)
	if err != nil {
		return false, err
	}
	if fmt.Sprint(exists) == "1" {
		return false, nil // 活行占用 → 冲突
	}
	// 占有行已死（过期未清扫）：自愈重占
	if _, err := g.cli.Do(ctx, "DEL", rkey); err != nil {
		return false, err
	}
	return true, g.retryReserve(ctx, rkey, owner)
}

func (g *Guard) retryReserve(ctx context.Context, rkey, owner string) error {
	res, err := g.cli.Do(ctx, "SET", rkey, owner, "NX")
	if err != nil {
		return err
	}
	if res == nil || fmt.Sprint(res) != "OK" {
		return fmt.Errorf("%w: reservation race on %s", kidb.ErrDuplicateKey, rkey)
	}
	return nil
}

// releaseUnique 释放预约：仅当占有者仍为本行（防误删他人新预约）。
func (g *Guard) releaseUnique(ctx context.Context, rkey, rowkey string) error {
	cur, err := g.cli.Do(ctx, "GET", rkey)
	if err != nil || cur == nil {
		return err
	}
	if strings.SplitN(fmt.Sprint(cur), "|", 2)[0] != rowkey {
		return nil
	}
	_, err = g.cli.Do(ctx, "DEL", rkey)
	return err
}

func (g *Guard) rollbackReservations(ctx context.Context, keys []string) {
	for _, k := range keys {
		_, _ = g.cli.Do(ctx, "DEL", k)
	}
}

// buildIndexOps 按旧行/新字段展开索引撤销与重建（ACTIVE 单桶形态；
// 分裂状态机的 SPLITTING/DRAINING 双写由控制器落地后扩展，docs/05 §5.1 第 4 步）。
func buildIndexOps(t *meta.TableDef, slot uint16, pk string, oldRow, newFields map[string]string) []indexOp {
	var ops []indexOp
	for _, idx := range t.Indexes {
		col := idx.Columns[0]
		oldVal, hadOld := oldRow[col]
		newVal, hasNew := newFields[col]

		// 等值/唯一：桶按值寻址；范围：单 ACTIVE 桶；字典序副本随行
		if idx.Kind == meta.IndexRange {
			op := indexOp{}
			if hadOld {
				op.undoKey = keycodec.RangeBucketKey(t.Name, idx.ID, slot, 0)
				op.undoMember = pk
			}
			if hasNew {
				score, err := strconv.ParseFloat(newVal, 64)
				if err == nil {
					op.redoKey = keycodec.RangeBucketKey(t.Name, idx.ID, slot, 0)
					op.redoMember = coveringMember(pk, idx, newFields)
					op.redoScore = score
					op.hasRedo = true
				}
			}
			ops = append(ops, op)
			continue
		}

		// IndexEq / IndexUnique
		op := indexOp{}
		if hadOld && (!hasNew || oldVal != newVal) {
			op.undoKey = keycodec.EqBucketKey(t.Name, idx.ID, oldVal, slot, 0)
			op.undoMember = pk
		}
		if hasNew {
			op.redoKey = keycodec.EqBucketKey(t.Name, idx.ID, newVal, slot, 0)
			op.redoMember = coveringMember(pk, idx, newFields)
			op.redoScore = 0
			op.hasRedo = true
		}
		ops = append(ops, op)

		// 字典序副本（前缀搜索，docs/04 §4.5）
		if idx.PrefixCopy {
			lop := indexOp{}
			if hadOld && (!hasNew || oldVal != newVal) {
				lop.undoKey = keycodec.LexBucketKey(t.Name, idx.ID, slot, 0)
				lop.undoMember = lexMember(oldVal, pk)
			}
			if hasNew {
				lop.redoKey = keycodec.LexBucketKey(t.Name, idx.ID, slot, 0)
				lop.redoMember = lexMember(newVal, pk)
				lop.redoScore = 0
				lop.hasRedo = true
			}
			ops = append(ops, lop)
		}
	}
	return ops
}

// coveringMember 桶 member 编码：无覆盖列 = pk；
// 有覆盖列 = pk + "|" + 覆盖列编码（docs/03 §3.5；
// TODO(impl): 覆盖列编码切换 msgp 代码生成版）。
func coveringMember(pk string, idx meta.IndexDef, fields map[string]string) string {
	if len(idx.Covering) == 0 {
		return pk
	}
	parts := make([]string, 0, len(idx.Covering)+1)
	parts = append(parts, pk)
	for _, c := range idx.Covering {
		parts = append(parts, fields[c])
	}
	return strings.Join(parts, "|")
}

// lexMember 字典序副本 member：value + \x00 + pk（按值字典序再按 pk 唯一）。
func lexMember(value, pk string) string {
	return value + "\x00" + pk
}

// mergeReceiptUndo 主键复活：把旧回执中的索引条目并入撤销集
// （回执字段 idx:<i> = bucketKey \x1f member，docs/07 §7.3）。
func mergeReceiptUndo(ops []indexOp, rcpt map[string]string) []indexOp {
	known := make(map[string]bool, len(ops))
	for _, d := range ops {
		if d.undoKey != "" {
			known[d.undoKey+"\x1f"+d.undoMember] = true
		}
	}
	for f, v := range rcpt {
		if !strings.HasPrefix(f, "idx:") {
			continue
		}
		parts := strings.SplitN(v, "\x1f", 2)
		if len(parts) != 2 || known[v] {
			continue
		}
		ops = append(ops, indexOp{undoKey: parts[0], undoMember: parts[1]})
	}
	return ops
}

// assembleIndexArgs 把描述符编译为去重桶段 + ARGV 尾段（6 字段/索引）。
func assembleIndexArgs(ops []indexOp) (bucketKeys []string, argvTail []any) {
	idxOf := map[string]int{}
	ref := func(key string) int {
		if key == "" {
			return 0
		}
		if i, ok := idxOf[key]; ok {
			return i
		}
		bucketKeys = append(bucketKeys, key)
		idxOf[key] = len(bucketKeys)
		return len(bucketKeys)
	}
	for _, d := range ops {
		score := "0"
		if d.hasRedo {
			score = strconv.FormatFloat(d.redoScore, 'g', -1, 64)
		}
		argvTail = append(argvTail,
			"E", // kind 自描述（v1 Lua 分支不区分，见脚本注释）
			strconv.Itoa(ref(d.undoKey)), d.undoMember,
			strconv.Itoa(ref(d.redoKey)), d.redoMember, score,
		)
	}
	return bucketKeys, argvTail
}

func (g *Guard) hgetall(ctx context.Context, key string) (map[string]string, error) {
	res, err := g.cli.Do(ctx, "HGETALL", key)
	if err != nil || res == nil {
		return nil, err
	}
	out := map[string]string{}
	switch v := res.(type) {
	case map[string]string:
		return v, nil
	case map[any]any:
		for k, val := range v {
			out[fmt.Sprint(k)] = fmt.Sprint(val)
		}
		return out, nil
	case []any:
		for i := 0; i+1 < len(v); i += 2 {
			out[fmt.Sprint(v[i])] = fmt.Sprint(v[i+1])
		}
		return out, nil
	}
	return nil, fmt.Errorf("txguard: unexpected HGETALL reply %T", res)
}

func oldVerOf(oldRow map[string]string) uint64 {
	if len(oldRow) == 0 {
		return 0
	}
	return parseUint64(oldRow["_ver"])
}

func parseUint64(s string) uint64 {
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

func contains(sp *[]string, s string) bool {
	for _, x := range *sp {
		if x == s {
			return true
		}
	}
	return false
}
