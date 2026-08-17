package controller

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"kidb/bucketmap"
	"kidb/keycodec"
	"kidb/kv"
	"kidb/meta"
	"kidb/metrics"
	"kidb/rowcodec"
	"kidb/tuning"
	"kidb/utils"
)

// reconcile.go：对账角色（docs/12 §12.8，v7.0）——周期抽样比对"数据侧推导"与
// "索引实际内容"，漂移指标化。设计纪律：
//
//   - **正常 = 0**：任何漂移都是内核 bug 信号；本角色只观测（指标 + 告警日志），
//     不自动修复——**v7.0 起两个显式例外**（均为"已提交内容的幂等重放"，不产生新事实）：
//     ① member 版本漂移（member.ver ≠ 行._ver——两段写并发交错的脏残留）
//     → 观测 + 自动清理（ZREM 精确旧 member，幂等）；
//     ② 登记册缺失（桶成员无 exp 登记——主 pipeline 第三段崩溃窗口）
//     → 观测 + 自动补登（ZADD exp，score 由行 PTTL 推导，幂等）；
//   - 正向检查（活行 → 索引成员必须存在且 score 相符）：同步索引无合法窗口，
//     缺失/错值即真漂移；
//   - 反向检查（桶成员 → 行）：只在"行已死且回执已清（清扫完成）仍残留"时计数——
//     TTL 清扫滞后窗口是合法暂态，不算漂移；
//   - 异步索引与 Building 索引跳过（合法滞后/回填窗口，docs/12 §12.8 声明）。
//
// 采样面（v7.0 集中册）：每 tick 每表抽 slots_per_tick 页，每页随机分册 +
// 随机 offset 取 rows_per_slot 行（tuning.toml [controller] reconcile_*）。

// Reconciler 对账器。随机源可注入（测试确定性）。
type Reconciler struct {
	cli   kv.Client
	store *meta.CatalogStore
	bm    *bucketmap.Store
	m     *metrics.Metrics
	rng   *rand.Rand
}

// NewReconciler 构造（m 可为 nil）。
func NewReconciler(cli kv.Client, store *meta.CatalogStore, bm *bucketmap.Store, m *metrics.Metrics) *Reconciler {
	return &Reconciler{
		cli: cli, store: store, bm: bm, m: m,
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Tick 一轮对账（选举锁持有者驱动，见 gateway controllerRole）。
func (r *Reconciler) Tick(ctx context.Context) error {
	tables, err := r.store.ListTables(ctx)
	if err != nil {
		return err
	}
	pagesPerTick := tuning.Get().Controller.ReconcileSlotsPerTick
	for _, name := range tables {
		def, err := r.store.Load(ctx, name)
		if err != nil || def == nil {
			continue
		}
		for i := 0; i < pagesPerTick; i++ {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := r.ReconcilePage(ctx, def); err != nil {
				slog.Debug("kidb 对账页失败（下轮再来）", "table", name, "err", err)
			}
		}
	}
	return nil
}

// liveRow 活行（含 _ver——版本戳 member 的期望构造源）。
type liveRow struct {
	pk     string
	ver    uint64
	fields map[string]string
}

// ReconcilePage 对账一页（导出供测试定点对账）。
//
// 流程：集中登记册随机分册随机页取样 → 批量 HGETALL + 回执活性 →
// 正向成员/score 校验（版本戳精确 member）→ 反向桶成员校验（孤儿/版本漂移清理/
// 登记册缺失补登）→ 唯一预约占有者活性校验。全部 pipeline 有界批。
func (r *Reconciler) ReconcilePage(ctx context.Context, def *meta.TableDef) error {
	// 同步可见索引集合（异步/回填中跳过——合法滞后窗口）
	var indexes []*meta.IndexDef
	for i := range def.Indexes {
		idx := &def.Indexes[i]
		if idx.Async || idx.Building {
			continue
		}
		indexes = append(indexes, idx)
	}
	// DLQ 深度巡检（触发四③：异步补写硬失败的最终观测面——非零即漂移）
	for i := range def.Indexes {
		idx := &def.Indexes[i]
		if !idx.Async {
			continue
		}
		depth, err := r.cli.Do(ctx, "LLEN", keycodec.DLQKey(def.Name, idx.ID))
		if err != nil {
			continue
		}
		if n, _ := strconv.ParseInt(fmt.Sprint(depth), 10, 64); n > 0 {
			r.drift("index_dlq_nonempty", def.Name, idx.ID, strconv.FormatInt(n, 10))
		}
	}
	if len(indexes) == 0 {
		return nil
	}

	// 1. 登记册取样（随机分册 + 随机页——首页恒取最早到期批次的偏倚已修正）
	rowsPerSlot := tuning.Get().Controller.ReconcileRowsPerSlot
	shards := def.EffectiveExpShards()
	shard := r.rng.Intn(shards)
	expKey := keycodec.ExpKeyN(def.Name, shard, shards)
	card, err := r.cli.Do(ctx, "ZCARD", expKey)
	if err != nil {
		return err
	}
	total, _ := strconv.ParseInt(fmt.Sprint(card), 10, 64)
	if total == 0 {
		return nil
	}
	off := int64(0)
	if total > int64(rowsPerSlot) {
		off = r.rng.Int63n(total - int64(rowsPerSlot))
	}
	res, err := r.cli.Do(ctx, "ZRANGE", expKey, off, off+int64(rowsPerSlot)-1)
	if err != nil {
		return err
	}
	pks := utils.Strings(res)
	if len(pks) == 0 {
		return nil
	}

	// 2. 批量读行（HGETALL——_ver 与覆盖列是期望 member 的构造源）+ 回执存在性
	var cmds []kv.Cmd
	for _, pk := range pks {
		cmds = append(cmds,
			kv.Cmd{Name: "HGETALL", Args: []any{keycodec.RowKey(def.Name, pk)}},
			kv.Cmd{Name: "EXISTS", Args: []any{keycodec.ReceiptKey(def.Name, pk)}})
	}
	results, err := r.cli.Pipeline(ctx, cmds)
	if err != nil {
		return err
	}

	var live []liveRow
	deadSwept := utils.NewSet[string]() // 死且已清扫
	for i, pk := range pks {
		fields, _ := utils.StringMap(results[2*i])
		if len(fields) == 0 { // 行不存在
			if fmt.Sprint(results[2*i+1]) != "1" {
				deadSwept.Add(pk)
			}
			continue
		}
		ver, _ := strconv.ParseUint(fields["_ver"], 10, 64)
		live = append(live, liveRow{pk: pk, ver: ver, fields: fields})
	}
	liveByPK := map[string]liveRow{}
	for _, row := range live {
		liveByPK[row.pk] = row
	}

	// 3. 正向：活行的索引成员必须存在（同步索引无合法缺失窗口；版本戳精确 member）。
	// 同 (pk,索引,值) 的检查跨桶展开（分裂窗口双读），任一桶命中即存在。
	type fwdCheck struct {
		pk, desc   string
		wantScore  float64 // 范围索引：score 必须相符（错值=真漂移）
		checkScore bool
	}
	var fcmds []kv.Cmd
	var checks []fwdCheck
	for _, row := range live {
		for _, idx := range indexes {
			encVal := row.fields[idx.Columns[0]]
			if encVal == "" {
				continue
			}
			d, err := r.bm.Load(ctx, def.Name, idx.ID)
			if err != nil {
				continue // bm 读失败不误报：本索引本轮跳过
			}
			switch idx.Kind {
			case meta.IndexEq, meta.IndexUnique:
				member := expectedMember(idx, row)
				for _, b := range d.ReadBucketsEq(keycodec.EscapeValue(encVal)) {
					fcmds = append(fcmds, kv.Cmd{Name: "ZSCORE", Args: []any{keycodec.EqBucketKey(def.Name, idx.ID, encVal, b), member}})
					checks = append(checks, fwdCheck{pk: row.pk, desc: idx.ID + "=" + encVal})
				}
				if idx.PrefixCopy {
					lm := rowcodec.LexMember(encVal, row.pk, row.ver)
					for _, b := range d.ReadBucketsEq("l") { // "l" 伪条目承载副本分裂状态（写路径同一约定）
						fcmds = append(fcmds, kv.Cmd{Name: "ZSCORE", Args: []any{keycodec.LexBucketKey(def.Name, idx.ID, b), lm}})
						checks = append(checks, fwdCheck{pk: row.pk, desc: idx.ID + "#lex=" + encVal})
					}
				}
			case meta.IndexRange:
				col, _ := def.Column(idx.Columns[0])
				score, err := rowcodec.ScoreOf(col.Type, encVal)
				if err != nil {
					continue
				}
				member := expectedMember(idx, row)
				for _, b := range d.ReadBucketsRange(score, score) {
					fcmds = append(fcmds, kv.Cmd{Name: "ZSCORE", Args: []any{keycodec.RangeBucketKey(def.Name, idx.ID, b), member}})
					checks = append(checks, fwdCheck{pk: row.pk, desc: idx.ID, wantScore: score, checkScore: true})
				}
			}
		}
	}
	if len(fcmds) > 0 {
		fres, err := r.cli.Pipeline(ctx, fcmds)
		if err != nil {
			return err
		}
		hit := utils.NewSet[string]()      // pk|desc → 存在
		badScore := utils.NewSet[string]() // pk|desc → score 不符
		for i, cres := range fres {
			if cres == nil {
				continue
			}
			c := checks[i]
			k := c.pk + "|" + c.desc
			hit.Add(k)
			if c.checkScore {
				got, err := strconv.ParseFloat(fmt.Sprint(cres), 64)
				if err != nil || got != c.wantScore {
					badScore.Add(k)
				}
			}
		}
		seen := utils.NewSet[string]()
		for _, c := range checks {
			k := c.pk + "|" + c.desc
			if seen.Has(k) {
				continue
			}
			seen.Add(k)
			switch {
			case !hit.Has(k):
				r.drift("index_member_missing", def.Name, c.desc, c.pk)
			case badScore.Has(k):
				r.drift("index_score_mismatch", def.Name, c.desc, c.pk)
			}
		}
	}

	// 4. 反向：等值/唯一桶成员 → 行状态（死且已清扫 = 残留泄漏；
	// 版本漂移 = 脏 member 自动清理（例外①）；登记册缺失 = 自动补登（例外②））。
	// 范围桶反向由正向的 score 校验覆盖（桶级反查边际收益低，不重复扇出）。
	var rcmds []kv.Cmd
	var rchecks []struct {
		bucket   string
		idxID    string
		covering bool
	}
	doneBucket := utils.NewSet[string]()
	for _, row := range live {
		for _, idx := range indexes {
			if idx.Kind == meta.IndexRange {
				continue
			}
			encVal := row.fields[idx.Columns[0]]
			if encVal == "" {
				continue
			}
			d, err := r.bm.Load(ctx, def.Name, idx.ID)
			if err != nil {
				continue
			}
			for _, b := range d.ReadBucketsEq(keycodec.EscapeValue(encVal)) {
				bk := keycodec.EqBucketKey(def.Name, idx.ID, encVal, b)
				if doneBucket.Has(bk) {
					continue
				}
				doneBucket.Add(bk)
				rcmds = append(rcmds, kv.Cmd{Name: "ZRANGE", Args: []any{bk, 0, 63}})
				rchecks = append(rchecks, struct {
					bucket   string
					idxID    string
					covering bool
				}{bucket: bk, idxID: idx.ID, covering: len(idx.Covering) > 0})
			}
		}
	}
	if len(rcmds) > 0 {
		rres, err := r.cli.Pipeline(ctx, rcmds)
		if err != nil {
			return err
		}
		// 候选收集：孤儿方向（行死且回执清）/ 版本漂移方向（member.ver ≠ _iv）/
		// 登记册缺失方向（无 exp 登记）
		cand := map[string]string{} // pk → bucket key（日志描述）
		var driftClean []kv.Cmd     // 版本漂移清理（ZREM 精确脏 member，幂等）
		expCheck := utils.NewSet[string]()
		for i, cres := range rres {
			for _, member := range utils.Strings(cres) {
				pk := rowcodec.MemberPK(member, rchecks[i].covering)
				row, isLive := liveByPK[pk]
				if isLive {
					// 版本判定基准 = _iv（member 创建版本，非当前行版本，docs/05 §5.1）
					mver, ok := rowcodec.MemberVer(member, rchecks[i].covering)
					if ok {
						wantVer := row.ver
						if v, err := strconv.ParseUint(row.fields["_iv:"+rchecks[i].idxID], 10, 64); err == nil && v > 0 {
							wantVer = v
						}
						if mver != wantVer {
							// 例外①：member 版本漂移（两段写交错脏残留）→ 观测 + 幂等清理
							r.drift("index_member_ver_stale", def.Name, rchecks[i].bucket, pk)
							driftClean = append(driftClean, kv.Cmd{Name: "ZREM", Args: []any{rchecks[i].bucket, member}})
						}
					}
					expCheck.Add(pk) // 登记册缺失方向只覆盖活行（幽灵/死行归孤儿与清扫面）
					continue
				}
				if deadSwept.Has(pk) {
					continue // 死且已清扫（v6 语义：清扫完成侧由 sweeper 负责）
				}
				if _, ok := cand[pk]; !ok {
					cand[pk] = rchecks[i].bucket
				}
			}
		}
		if len(driftClean) > 0 {
			if _, err := r.cli.Pipeline(ctx, driftClean); err != nil {
				return err
			}
		}
		// 孤儿方向：直查行/回执活性（从未存在的 pk 无从判定）
		var ocmds []kv.Cmd
		var opks []string
		for pk := range cand {
			ocmds = append(ocmds,
				kv.Cmd{Name: "EXISTS", Args: []any{keycodec.RowKey(def.Name, pk)}},
				kv.Cmd{Name: "EXISTS", Args: []any{keycodec.ReceiptKey(def.Name, pk)}})
			opks = append(opks, pk)
		}
		if len(ocmds) > 0 {
			ores, err := r.cli.Pipeline(ctx, ocmds)
			if err != nil {
				return err
			}
			for i, pk := range opks {
				rowAlive := fmt.Sprint(ores[2*i]) == "1"
				rcptAlive := fmt.Sprint(ores[2*i+1]) == "1"
				if !rowAlive && !rcptAlive {
					r.drift("index_member_orphan", def.Name, cand[pk], pk)
				}
			}
		}
		// 例外②：登记册缺失（桶成员无 exp 登记——主 pipeline 第三段崩溃窗口）
		// → 观测 + 幂等补登（score 由行 PTTL 推导）
		var ecmds []kv.Cmd
		var epks []string
		for pk := range expCheck {
			ecmds = append(ecmds, kv.Cmd{Name: "ZSCORE", Args: []any{expKey, pk}})
			epks = append(epks, pk)
		}
		if len(ecmds) > 0 {
			eres, err := r.cli.Pipeline(ctx, ecmds)
			if err != nil {
				return err
			}
			var tcmds []kv.Cmd
			var tpks []string
			for i, er := range eres {
				if er == nil {
					tcmds = append(tcmds, kv.Cmd{Name: "PTTL", Args: []any{keycodec.RowKey(def.Name, epks[i])}})
					tpks = append(tpks, epks[i])
				}
			}
			if len(tcmds) > 0 {
				tres, err := r.cli.Pipeline(ctx, tcmds)
				if err != nil {
					return err
				}
				now := time.Now().Unix()
				var backfill []kv.Cmd
				for i, tr := range tres {
					pttl, _ := strconv.ParseInt(fmt.Sprint(tr), 10, 64)
					if pttl == -2 { // 行已死（竞态窗口）——不补登，归孤儿/清扫面
						continue
					}
					score := any("+inf")
					if pttl > 0 {
						score = now + pttl/1000
					}
					r.drift("exp_registry_missing", def.Name, expKey, tpks[i])
					backfill = append(backfill, kv.Cmd{Name: "ZADD", Args: []any{expKey, score, tpks[i]}})
				}
				if len(backfill) > 0 {
					if _, err := r.cli.Pipeline(ctx, backfill); err != nil {
						return err
					}
				}
			}
		}
	}

	// 5. 唯一预约巡检（双向）：残留 = 预约在而占有行死；缺失 = 活行无预约
	// （缺失侧 review 实证：CREATE UNIQUE INDEX 回填曾不建预约，存量值对
	// 唯一约束不可见——正向桶成员检查照不出这个洞，必须单独断言）。
	var ucmds []kv.Cmd
	var ukeys []string
	for _, row := range live {
		for _, idx := range indexes {
			if idx.Kind != meta.IndexUnique {
				continue
			}
			encVal := row.fields[idx.Columns[0]]
			if encVal == "" {
				continue
			}
			uk := keycodec.UniqueKey(def.Name, idx.ID, encVal)
			ucmds = append(ucmds, kv.Cmd{Name: "GET", Args: []any{uk}})
			ukeys = append(ukeys, uk)
		}
	}
	if len(ucmds) > 0 {
		ures, err := r.cli.Pipeline(ctx, ucmds)
		if err != nil {
			return err
		}
		var ocmds []kv.Cmd
		var okeys []string
		for i, ures1 := range ures {
			if ures1 == nil {
				r.drift("uniq_reservation_missing", def.Name, ukeys[i], "") // 活行无预约
				continue
			}
			owner := strings.SplitN(fmt.Sprint(ures1), "|", 2)[0] // 占有者行 key
			ocmds = append(ocmds, kv.Cmd{Name: "EXISTS", Args: []any{owner}})
			okeys = append(okeys, ukeys[i])
		}
		if len(ocmds) > 0 {
			ores, err := r.cli.Pipeline(ctx, ocmds)
			if err != nil {
				return err
			}
			for i, ores1 := range ores {
				if fmt.Sprint(ores1) == "0" {
					r.drift("uniq_reservation_residual", def.Name, okeys[i], "")
				}
			}
		}
	}
	return nil
}

// expectedMember 构造期望 member（版本戳精确，docs/05 §5.1）：
// 版本取行内 `_iv:{idxID}`（member 创建版本——值最后一次写入时的行版本，
// 缺失回退当前行版本，与 txguard ivOf 同一纪律）。
func expectedMember(idx *meta.IndexDef, row liveRow) string {
	ver := row.ver
	if v, err := strconv.ParseUint(row.fields["_iv:"+idx.ID], 10, 64); err == nil && v > 0 {
		ver = v
	}
	if len(idx.Covering) == 0 {
		return rowcodec.PlainMember(row.pk, ver)
	}
	covers := make([]string, 0, len(idx.Covering))
	for _, c := range idx.Covering {
		covers = append(covers, row.fields[c])
	}
	return rowcodec.EncodeMember(row.pk, ver, covers)
}

// drift 记录一次漂移（指标 + 告警；观测纪律，两个幂等例外见头注）。
func (r *Reconciler) drift(kind, table, where, pk string) {
	if r.m != nil {
		r.m.ReconcileDrift.WithLabelValues(kind).Inc()
	}
	slog.Warn("kidb 对账漂移", "kind", kind, "table", table, "where", where, "pk", pk)
}
