// Package txguard 是写入路径的编排层（docs/05，v7.0 两段写协议）：
// 预读旧行/旧回执 → 展开撤/建清单（成员带版本戳）→ 唯一预约（SET NX PX 两阶段）
// → 行本地 Lua（行/回执/异步日志单 slot 原子）→ 索引命令组 pipeline（异 slot，
// 版本戳幂等——stale 重试安全，并发交错不漏（docs/05 §5.1 不变式））。
// stale（版本冲突）整体重试 ≤3 次。
package txguard

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"kidb"
	"kidb/bucketmap"
	"kidb/i18n"
	"kidb/keycodec"
	"kidb/kv"
	"kidb/meta"
	"kidb/metrics"
	"kidb/rowcodec"
	"kidb/script"
	"kidb/tuning"
	"kidb/utils"
)

// maxStaleRetries 由调用点读取 tuning.Get().Txguard.StaleRetries（docs/05 §5.5）。

// uniqueResvTTL 唯一预约 PX（v7.0 触发四②：24h 内置——Sweeper 空窗期预约
// 无人释放时由时间自愈兜底，docs/05 §5.3）。
const uniqueResvTTL = 24 * time.Hour

// Guard 编排单行写入。通过 kv.Client 下发命令，满足契约 R1~R7。
type Guard struct {
	cli   kv.Client
	reg   *script.Registry
	bm    *bucketmap.Store // 桶路由（分裂状态），nil = 永远 ACTIVE 单桶
	m     *metrics.Metrics // 指标（nil = no-op）
	clock func() time.Time // 测试可注入
}

// New 构造 Guard（bm 供分裂状态路由；传 nil 退化为 ACTIVE 单桶模式）。
func New(cli kv.Client, reg *script.Registry, bm *bucketmap.Store) *Guard {
	return &Guard{cli: cli, reg: reg, bm: bm, clock: time.Now}
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
	KeepTTL        bool          // UPDATE 不提 _ttl：行 TTL/登记册照旧（write_row.lua ttlms=-2）
	ExpectedOldVer *int64        // nil = 不校验；非 nil = CAS 写语义（docs/05 §5.6）
}

// Result 是写入结果。
type Result struct {
	OldVer       uint64
	NewVer       uint64
	StaleRetries int
}

// 撤/建清单条目（v7.0 两段写）：
//   - undoEntry.member 为精确旧 member（含旧版本戳，ZREM 幂等）；
//   - redoEntry.member 为含**预期新版本戳**（oldVer+1）的完整 member——
//     行 Lua 经 expectOld 校验后 HINCRBY 必得该版本（stale 则 Lua 整体未写，
//     索引段同 pipeline 产物必为"多"，读取去重/回表过滤/对账清理兜底）。
type undoEntry struct{ bucket, member string }
type redoEntry struct {
	bucket, member string
	score          float64
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
			return Result{}, fmt.Errorf("%w: %s", kidb.ErrContractViolation, i18n.T("tx.pk_in_fields", f))
		}
	}
	rowkey := keycodec.RowKey(req.Table.Name, req.PK)

	var acquired []string // 本事务占有的唯一预约 key（回滚/重试用）
	var res Result
	for attempt := 0; attempt < tuning.Get().Txguard.StaleRetries; attempt++ {
		res.StaleRetries = attempt
		stale, err := g.writeAttempt(ctx, req, rowkey, &acquired, &res)
		if err != nil {
			g.rollbackReservations(ctx, acquired, rowkey)
			return Result{}, err
		}
		if !stale {
			g.hllSample(ctx, req) // 索引基数统计采样（docs/04 §4.6：统计可以近似）
			return res, nil
		}
		// stale：行被并发改写，整体重读重试。
		// bm 缓存可能持旧版本——先失效再重试（预约 key 我方已持有，重试幂等）。
		if g.bm != nil {
			g.bm.Invalidate()
		}
		if g.m != nil {
			g.m.LuaStaleRetry.Inc() // lua_stale_retry_total
		}
	}
	g.rollbackReservations(ctx, acquired, rowkey)
	return Result{}, fmt.Errorf("%w: write %s after %d attempts", kidb.ErrStaleMetadata, rowkey, tuning.Get().Txguard.StaleRetries)
}

// writeAttempt 完成一轮"预读 → 展开 → 预约 → 行 Lua → 索引段"；stale=true 表示版本冲突。
func (g *Guard) writeAttempt(ctx context.Context, req WriteReq, rowkey string,
	acquired *[]string, res *Result) (stale bool, err error) {

	t := req.Table
	slot := keycodec.Slot(rowkey)
	rcptkey := keycodec.ReceiptKey(t.Name, req.PK)

	// 预读旧行；旧行空则预读回执（主键复活：撤销条目按回执展开，docs/05 §5.1）
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

	docs, err := g.loadDocs(ctx, t)
	if err != nil {
		return false, err
	}

	oldVer := oldVerOf(oldRow)
	newVer := oldVer + 1
	undos, redos, asyncs := buildIndexOps(t, slot, req.PK, oldRow, req.Fields, docs, oldVer, newVer)
	// 复活路径：把旧回执的索引条目并入撤销集（回执内为精确 member，原样使用）
	if len(oldRow) == 0 && len(oldRcpt) > 0 {
		undos = mergeReceiptUndo(undos, oldRcpt)
	}

	// 唯一预约（docs/05 §5.3：按值散列 SET NX PX + 占有者活检查 + 自愈）
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
		if !slices.Contains(*acquired, rkey) {
			*acquired = append(*acquired, rkey)
		}
		uniqEntries = append(uniqEntries, [2]string{idx.ID, rkey})
	}

	// 撤字段面（write_row v6 起）：旧有新无 = UPDATE 置 NULL——HSET 不覆盖即
	// 幽灵残留，展开为显式 HDEL 清单（_ver 是内部字段，永不撤销）。
	var dropped []string
	for f := range oldRow {
		if f == "_ver" {
			continue
		}
		if _, ok := req.Fields[f]; !ok {
			dropped = append(dropped, f)
		}
	}

	// _ver 语义（docs/05 §5.6）：
	//  - ARGV[4] 恒为本次预读观察到的 old_ver——Lua 内再读不符说明预读→提交间
	//    有并发写入，stale 让网关以新旧行重展开整体重试；
	//  - 调用方 ExpectedOldVer 非 nil 是 CAS 写语义：预读版本不符即 fail-fast，
	//    不重试（重试不会改变调用方的过期预期）。
	observed := int64(oldVer)
	if req.ExpectedOldVer != nil && *req.ExpectedOldVer != observed {
		return false, fmt.Errorf("%w: %s",
			kidb.ErrStaleMetadata, i18n.T("tx.ver_mismatch", rowkey, *req.ExpectedOldVer, observed))
	}
	ttlms := int64(req.TTL / time.Millisecond)
	if req.KeepTTL {
		ttlms = -2 // UPDATE 保留语义（docs/07 §7.1）
	} else if req.TTL < 0 {
		ttlms = 1 // 软删除：立即过期走清扫
	}

	// 行 Lua（单 slot 原子面：行/回执/异步日志）
	tn := tuning.Get()
	logKeys := make([]string, 0, len(asyncs))
	for _, a := range asyncs {
		logKeys = append(logKeys, a.logKey)
	}
	keys := append([]string{rowkey, rcptkey}, logKeys...)
	argv := []any{
		"W", req.PK, strconv.FormatInt(ttlms, 10),
		strconv.FormatInt(observed, 10),
		strconv.Itoa(tn.Txguard.AsyncLogCapacity), strconv.Itoa(tn.Sweeper.ReceiptGraceMs),
		strconv.Itoa(len(asyncs)),
	}
	for _, a := range asyncs {
		argv = append(argv, logKeyIdx(a.logKey, logKeys), a.redoMember)
	}
	argv = append(argv, strconv.Itoa(len(redos)))
	for _, r := range redos {
		argv = append(argv, r.bucket, r.member)
	}
	argv = append(argv, strconv.Itoa(len(req.Fields)))
	for f, v := range req.Fields {
		argv = append(argv, f, v)
	}
	argv = append(argv, strconv.Itoa(len(dropped)))
	for _, f := range dropped {
		argv = append(argv, f)
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
	case "log_full":
		return false, fmt.Errorf("%w: %s", kidb.ErrIndexLogFull, i18n.T("tx.index_log_full", rowkey))
	case "ok":
		res.OldVer = utils.ParseUint64(fmt.Sprint(arr[1]))
		if len(arr) > 2 {
			res.NewVer = utils.ParseUint64(fmt.Sprint(arr[2]))
		}
	default:
		return false, fmt.Errorf("txguard: write_row unknown status %v", arr[0])
	}

	// 索引命令组（异 slot，版本戳精确 member；docs/05 §5.1）+ 登记册段
	cmds := make([]kv.Cmd, 0, len(undos)+len(redos)+1)
	for _, u := range undos {
		cmds = append(cmds, kv.Cmd{Name: "ZREM", Args: []any{u.bucket, u.member}})
	}
	for _, r := range redos {
		cmds = append(cmds, kv.Cmd{Name: "ZADD", Args: []any{r.bucket, r.score, r.member}})
	}
	expShards := t.EffectiveExpShards()
	expKey := keycodec.ExpKeyN(t.Name, keycodec.ExpShardFor(req.PK, expShards), expShards)
	switch {
	case ttlms > 0:
		cmds = append(cmds, kv.Cmd{Name: "ZADD", Args: []any{expKey, now.Unix() + ttlms/1000, req.PK}})
	case ttlms == 0:
		cmds = append(cmds, kv.Cmd{Name: "ZADD", Args: []any{expKey, "+inf", req.PK}})
	case ttlms == -2 && len(oldRow) == 0:
		cmds = append(cmds, kv.Cmd{Name: "ZADD", Args: []any{expKey, "+inf", req.PK}}) // 新行无可保 TTL，登记不过期
	}
	if _, err := g.cli.Pipeline(ctx, cmds); err != nil {
		// 第二段失败 = 报错（Redis 命令失败语义，docs/05 §5.1）——行已提交，
		// 客户端重试幂等收敛（ZADD/ZREM 对精确 member 幂等）；崩溃类窗口由对账兜底。
		return false, err
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
			if strings.HasPrefix(f, "__uniq:") && !slices.Contains(*acquired, v) {
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
	for attempt := 0; attempt < tuning.Get().Txguard.StaleRetries; attempt++ {
		oldRow, err := g.hgetall(ctx, rowkey)
		if err != nil {
			return false, err
		}
		if len(oldRow) == 0 {
			return false, nil // 0 rows affected（预读空即不存在；复活竞态由 Lua 内复查覆盖不到——
			// 预读→Lua 间新插入的行属新一轮写入，其索引成员由写入方负责，删除不带撤销是诚实声明的边界）
		}
		docs, err := g.loadDocs(ctx, t)
		if err != nil {
			return false, err
		}
		oldVer := oldVerOf(oldRow)
		undos, _, asyncs := buildIndexOps(t, slot, pk, oldRow, nil, docs, oldVer, oldVer+1)

		logKeys := make([]string, 0, len(asyncs))
		for _, a := range asyncs {
			logKeys = append(logKeys, a.logKey)
		}
		keys := append([]string{rowkey, keycodec.ReceiptKey(t.Name, pk)}, logKeys...)
		tn := tuning.Get()
		argv := []any{
			"D", pk, "0",
			strconv.FormatUint(oldVer, 10),
			strconv.Itoa(tn.Txguard.AsyncLogCapacity), strconv.Itoa(tn.Sweeper.ReceiptGraceMs),
			strconv.Itoa(len(asyncs)),
		}
		for _, a := range asyncs {
			argv = append(argv, logKeyIdx(a.logKey, logKeys), a.redoMember)
		}

		wr, _ := g.reg.Get("write_row")
		out, err := g.cli.Eval(ctx, wr, keys, argv...)
		if err != nil {
			return false, err
		}
		arr, _ := out.([]any)
		if len(arr) > 0 && fmt.Sprint(arr[0]) == "stale" {
			if g.bm != nil {
				g.bm.Invalidate()
			}
			continue
		}
		existed := len(arr) > 1 && fmt.Sprint(arr[1]) == "1"
		if !existed {
			return false, nil
		}

		// 索引撤销段（精确旧 member）+ 登记册移除
		cmds := make([]kv.Cmd, 0, len(undos)+1)
		for _, u := range undos {
			cmds = append(cmds, kv.Cmd{Name: "ZREM", Args: []any{u.bucket, u.member}})
		}
		expShards := t.EffectiveExpShards()
		cmds = append(cmds, kv.Cmd{Name: "ZREM", Args: []any{
			keycodec.ExpKeyN(t.Name, keycodec.ExpShardFor(pk, expShards), expShards), pk}})
		if _, err := g.cli.Pipeline(ctx, cmds); err != nil {
			return false, err
		}

		// 提交成功：释放该行的唯一预约（异 slot DEL）
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
	return false, fmt.Errorf("%w: delete %s after %d attempts", kidb.ErrStaleMetadata, rowkey, tuning.Get().Txguard.StaleRetries)
}

// ReserveUniqueForBackfill 供 DDL 回填为存量行补建唯一预约（docs/06 §6.3）。
// 与写路径同一预约纪律（SET NX PX + 占有者活检查 + 死占有者自愈）；
// ok=false = 真实冲突（占有者活行存在）——调用方据此中止建索引作业。
func (g *Guard) ReserveUniqueForBackfill(ctx context.Context, t *meta.TableDef, idxID, encVal, pk string) (bool, error) {
	rkey := keycodec.UniqueKey(t.Name, idxID, encVal)
	rowkey := keycodec.RowKey(t.Name, pk)
	return g.reserveUnique(ctx, rkey, rowkey, g.clock())
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

// reserveUnique 执行唯一预约两阶段（docs/05 §5.3，v7.0 触发四②带 PX 自愈）：
// SET NX PX → 失败则读占有者 → EXISTS 判活：活=false 返回；死=自愈重占。
// 占有者就是本行（重试场景）视为成功。
func (g *Guard) reserveUnique(ctx context.Context, rkey, rowkey string, now time.Time) (bool, error) {
	owner := rowkey + "|" + strconv.FormatInt(now.Unix(), 10)
	res, err := g.cli.Do(ctx, "SET", rkey, owner, "NX", "PX", uniqueResvTTL.Milliseconds())
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
		return true, g.retryReserve(ctx, rkey, owner) // 刚好被释放/到期
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
	res, err := g.cli.Do(ctx, "SET", rkey, owner, "NX", "PX", uniqueResvTTL.Milliseconds())
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

// rollbackReservations 回滚本次占有的预约（错误路径）。
// 占有者比对（releaseUnique 同款）而不是裸 DEL：歧义超时（Lua 或已提交）下
// 预约可能已被自愈/继任者占有，裸 DEL 会误杀活预约（review 实证窗口）。
// 注意诚实边界：歧义超时后"行已提交但预约被我方回滚"的极小窗口仍存——
// 由对账 uniq_reservation_missing 观测，docs/05 §5.3 已知窗口如实声明。
func (g *Guard) rollbackReservations(ctx context.Context, keys []string, rowkey string) {
	for _, k := range keys {
		_ = g.releaseUnique(ctx, k, rowkey)
	}
}

// loadDocs 加载本次写入涉及的全部同步索引的 BucketMap 文档（v7.0 集中形态）。
func (g *Guard) loadDocs(ctx context.Context, t *meta.TableDef) (map[string]*bucketmap.Doc, error) {
	docs := map[string]*bucketmap.Doc{}
	if g.bm == nil {
		return docs, nil
	}
	for _, idx := range t.Indexes {
		if idx.Async {
			continue
		}
		d, err := g.bm.Load(ctx, t.Name, idx.ID)
		if err != nil {
			return nil, err
		}
		docs[idx.ID] = d
	}
	return docs, nil
}

// asyncDesc 异步日志描述符（行 Lua 面）。
type asyncDesc struct {
	logKey     string
	redoMember string
}

// buildIndexOps 按旧行/新字段展开撤/建清单（v7.0：成员带版本戳；
// 双写规则由 bucketmap.Doc 路由规则给出，docs/08 §8.3）。
// oldVer/newVer 由调用方给定（newVer = oldVer+1——行 Lua expectOld 校验后的
// HINCRBY 必得；redo member 即按预期新版本戳构造）。
func buildIndexOps(t *meta.TableDef, slot uint16, pk string, oldRow, newFields map[string]string,
	docs map[string]*bucketmap.Doc, oldVer, newVer uint64) (undos []undoEntry, redos []redoEntry, asyncs []asyncDesc) {

	for _, idx := range t.Indexes {
		col := idx.Columns[0]
		oldVal, hadOld := oldRow[col]
		newVal, hasNew := newFields[col]
		d := docs[idx.ID] // nil（无 bm 模式）或默认文档 → 单桶

		// 异步分支：值有变化才记日志（墓碑 = 新值空串）
		if idx.Async {
			if hadOld == hasNew && (!hadOld || oldVal == newVal) {
				continue
			}
			asyncs = append(asyncs, asyncDesc{
				logKey:     keycodec.AsyncLogKey(t.Name, idx.ID, slot),
				redoMember: pk + "\x1f" + escLogField(oldVal) + "\x1f" + escLogField(newVal),
			})
			continue
		}

		switch idx.Kind {
		case meta.IndexRange:
			if hadOld {
				if oldScore, err := strconv.ParseFloat(oldVal, 64); err == nil {
					for _, b := range rangeReadSet(d, oldScore) {
						undos = append(undos, undoEntry{
							keycodec.RangeBucketKey(t.Name, idx.ID, b),
							coveringMember(pk, oldVer, idx, oldRow),
						})
					}
				}
			}
			if hasNew {
				if score, err := strconv.ParseFloat(newVal, 64); err == nil {
					for _, b := range rangeWriteSet(d, score) {
						redos = append(redos, redoEntry{
							keycodec.RangeBucketKey(t.Name, idx.ID, b),
							coveringMember(pk, newVer, idx, newFields), score,
						})
					}
				}
			}

		default: // IndexEq / IndexUnique
			if hadOld && (!hasNew || oldVal != newVal) {
				for _, b := range eqReadSet(d, keycodec.EscapeValue(oldVal)) {
					undos = append(undos, undoEntry{
						keycodec.EqBucketKey(t.Name, idx.ID, oldVal, b),
						coveringMember(pk, oldVer, idx, oldRow),
					})
				}
			}
			if hasNew {
				for _, b := range eqWriteSet(d, keycodec.EscapeValue(newVal), pk) {
					redos = append(redos, redoEntry{
						keycodec.EqBucketKey(t.Name, idx.ID, newVal, b),
						coveringMember(pk, newVer, idx, newFields), 0,
					})
				}
			}

			// 字典序副本随同等值索引分裂（"l" 条目，按 member 内 pk 同规则散列）
			if idx.PrefixCopy {
				if hadOld && (!hasNew || oldVal != newVal) {
					for _, b := range eqReadSet(d, "l") {
						undos = append(undos, undoEntry{
							keycodec.LexBucketKey(t.Name, idx.ID, b),
							rowcodec.LexMember(oldVal, pk, oldVer),
						})
					}
				}
				if hasNew {
					for _, b := range eqWriteSet(d, "l", pk) {
						redos = append(redos, redoEntry{
							keycodec.LexBucketKey(t.Name, idx.ID, b),
							rowcodec.LexMember(newVal, pk, newVer), 0,
						})
					}
				}
			}
		}
	}
	return undos, redos, asyncs
}

// eqReadSet / eqWriteSet / rangeReadSet / rangeWriteSet 是 bucketmap 路由规则的
// nil 安全包装（无 bm 时恒为默认单桶 [0]）。
func eqReadSet(d *bucketmap.Doc, encVal string) []int {
	if d == nil {
		return []int{0}
	}
	return d.ReadBucketsEq(encVal)
}

func eqWriteSet(d *bucketmap.Doc, encVal, pk string) []int {
	if d == nil {
		return []int{0}
	}
	return d.WriteTargetsEq(encVal, pk)
}

func rangeReadSet(d *bucketmap.Doc, score float64) []int {
	if d == nil {
		return []int{0}
	}
	return d.ReadBucketsRange(score, score)
}

func rangeWriteSet(d *bucketmap.Doc, score float64) []int {
	if d == nil {
		return []int{0}
	}
	return d.WriteTargetsRange(score)
}

// coveringMember 桶 member 编码（docs/03 §3.5，v7.0 版本戳）：
// 无覆盖列 = pk\x1fver；有覆盖列 = msgp 数组 [pk, ver, ...]（rowcodec 单点）。
func coveringMember(pk string, ver uint64, idx meta.IndexDef, fields map[string]string) string {
	if len(idx.Covering) == 0 {
		return rowcodec.PlainMember(pk, ver)
	}
	covers := make([]string, 0, len(idx.Covering))
	for _, c := range idx.Covering {
		covers = append(covers, fields[c])
	}
	return rowcodec.EncodeMember(pk, ver, covers)
}

// escLogField 异步日志字段转义：url.QueryEscape（**可逆**——Indexer 解回原始值
// 再按 keycodec 规则建桶 key/lex member）。review 实证教训：此前复用
// keycodec.EscapeValue（>128B 或含分隔符即摘要），摘要不可逆 → Indexer 二次转义
// 建出与查询侧错位的桶 key，含空格/中文的值对异步索引永久不可见。
func escLogField(v string) string { return url.QueryEscape(v) }

// logKeyIdx 异步日志 key 在 KEYS[3..] 段的相对序号（1 起）。
func logKeyIdx(key string, keys []string) string {
	for i, k := range keys {
		if k == key {
			return strconv.Itoa(i + 1)
		}
	}
	return "1"
}

// mergeReceiptUndo 主键复活：把旧回执中的索引条目并入撤销集
// （回执字段 idx:<i> = bucketKey \x1f member（含版本戳，原样精确），docs/07 §7.3）。
func mergeReceiptUndo(undos []undoEntry, rcpt map[string]string) []undoEntry {
	known := make(utils.Set[string], len(undos))
	for _, u := range undos {
		known.Add(u.bucket + "\x1f" + u.member)
	}
	for f, v := range rcpt {
		if !strings.HasPrefix(f, "idx:") {
			continue
		}
		parts := strings.SplitN(v, "\x1f", 2)
		if len(parts) != 2 || known.Has(v) {
			continue
		}
		undos = append(undos, undoEntry{parts[0], parts[1]})
	}
	return undos
}

// oldVerOf 读行内 _ver（无 = 0）。
func oldVerOf(row map[string]string) uint64 {
	v, _ := strconv.ParseUint(row["_ver"], 10, 64)
	return v
}

// hgetall 读行/回执全字段（契约 R4 前最小命令面）。
func (g *Guard) hgetall(ctx context.Context, key string) (map[string]string, error) {
	res, err := g.cli.Do(ctx, "HGETALL", key)
	if err != nil {
		return nil, err
	}
	return utils.StringMap(res)
}
