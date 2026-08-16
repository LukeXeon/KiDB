package controller

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"kidb"
	"kidb/bucketmap"
	"kidb/keycodec"
	"kidb/meta"
	"kidb/metrics"
	"kidb/rowcodec"
	"kidb/tuning"
	"kidb/utils"
)

// reconcile.go：对账角色（docs/12 §12.8）——周期抽样比对"数据侧推导"与
// "索引实际内容"，漂移指标化。设计纪律：
//
//   - **正常 = 0**：任何漂移都是内核 bug 信号；本角色只观测（指标 + 告警日志），
//     不自动修复（写路径自愈 + 读路径回表校验已是正确性机制，自动修复会掩盖 bug）；
//   - 正向检查（活行 → 索引成员必须存在且 score 相符）：同步索引无合法窗口，
//     缺失/错值即真漂移；
//   - 反向检查（桶成员 → 行）：只在"行已死且回执已清（清扫完成）仍残留"时计数——
//     TTL 清扫滞后窗口是合法暂态，不算漂移；
//   - 异步索引与 Building 索引跳过（合法滞后/回填窗口，docs/12 §12.8 声明）。
//
// 采样面：每 tick 每表抽 slots_per_tick 个 slot，每 slot 取登记册首页
// rows_per_slot 行（tuning.toml [controller] reconcile_*）。

// Reconciler 对账器。随机源可注入（测试确定性）。
type Reconciler struct {
	cli   kidb.KvClient
	store *meta.CatalogStore
	bm    *bucketmap.Store
	m     *metrics.Metrics
	rng   *rand.Rand
}

// NewReconciler 构造（m 可为 nil）。
func NewReconciler(cli kidb.KvClient, store *meta.CatalogStore, bm *bucketmap.Store, m *metrics.Metrics) *Reconciler {
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
	slotsPerTick := tuning.Get().Controller.ReconcileSlotsPerTick
	for _, name := range tables {
		def, err := r.store.Load(ctx, name)
		if err != nil || def == nil {
			continue
		}
		for i := 0; i < slotsPerTick; i++ {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := r.ReconcileSlot(ctx, def, uint16(r.rng.Intn(keycodec.NumSlots))); err != nil {
				slog.Debug("kidb 对账 slot 失败（下轮再来）", "table", name, "err", err)
			}
		}
	}
	return nil
}

// ReconcileSlot 对账单 slot（导出供测试定点对账）。
//
// 流程：登记册取样 → 批量 HMGET 索引列 + 回执活性 → 正向成员/score 校验 →
// 反向桶成员残留校验 → 唯一预约占有者活性校验。全部 pipeline 有界批。
func (r *Reconciler) ReconcileSlot(ctx context.Context, def *meta.TableDef, slot uint16) error {
	// 同步可见索引集合（异步/回填中跳过——合法滞后窗口）
	var indexes []*meta.IndexDef
	for i := range def.Indexes {
		idx := &def.Indexes[i]
		if idx.Async || idx.Building {
			continue
		}
		indexes = append(indexes, idx)
	}
	if len(indexes) == 0 {
		return nil
	}

	// 1. 登记册取样（首页 rows_per_slot 行）
	rowsPerSlot := tuning.Get().Controller.ReconcileRowsPerSlot
	expKey := keycodec.ExpKeyN(def.Name, slot, 0, def.EffectiveExpShards())
	res, err := r.cli.Do(ctx, "ZRANGE", expKey, 0, rowsPerSlot-1)
	if err != nil {
		return err
	}
	pks := utils.Strings(res)
	if len(pks) == 0 {
		return nil
	}

	// 2. 批量读行（HMGET 索引列）+ 回执存在性（死行分两态：回执在 = 清扫窗口
	// 合法暂态；回执不在 = 已清扫——其桶成员仍存即残留）
	var cmds []kidb.Cmd
	for _, pk := range pks {
		args := []any{keycodec.RowKey(def.Name, pk)}
		for _, idx := range indexes {
			args = append(args, idx.Columns[0])
		}
		cmds = append(cmds, kidb.Cmd{Name: "HMGET", Args: args})
		cmds = append(cmds, kidb.Cmd{Name: "EXISTS", Args: []any{keycodec.ReceiptKey(def.Name, pk)}})
	}
	results, err := r.cli.Pipeline(ctx, cmds)
	if err != nil {
		return err
	}

	type liveRow struct {
		pk   string
		vals []string // 与 indexes 对齐（编码形态；"" = NULL/列缺失）
	}
	var live []liveRow
	deadSwept := map[string]bool{} // 死且已清扫
	for i, pk := range pks {
		vals, allNil := hmgetStrings(results[2*i])
		rowAlive := !(len(vals) == 0 || allNil) // HMGET 全 nil = 行不存在
		if !rowAlive {
			if fmt.Sprint(results[2*i+1]) != "1" {
				deadSwept[pk] = true
			}
			continue
		}
		live = append(live, liveRow{pk: pk, vals: vals})
	}

	// 3. 正向：活行的索引成员必须存在（同步索引无合法缺失窗口）。
	// 同 (pk,索引,值) 的检查跨桶展开（分裂窗口双读），任一桶命中即存在。
	type fwdCheck struct {
		pk, desc   string
		wantScore  float64 // 范围索引：score 必须相符（错值=真漂移）
		checkScore bool
	}
	var fcmds []kidb.Cmd
	var checks []fwdCheck
	for _, row := range live {
		for j, idx := range indexes {
			encVal := row.vals[j]
			if encVal == "" {
				continue
			}
			sh, err := r.bm.Load(ctx, def.Name, idx.ID, slot)
			if err != nil {
				continue // bm 读失败不误报：本 slot 本轮跳过
			}
			switch idx.Kind {
			case meta.IndexEq, meta.IndexUnique:
				for _, b := range sh.ReadBucketsEq(encVal) {
					fcmds = append(fcmds, kidb.Cmd{Name: "ZSCORE", Args: []any{keycodec.EqBucketKey(def.Name, idx.ID, encVal, slot, b), row.pk}})
					checks = append(checks, fwdCheck{pk: row.pk, desc: idx.ID + "=" + encVal})
				}
				if idx.PrefixCopy {
					member := rowcodec.LexMember(encVal, row.pk)
					for _, b := range sh.ReadBucketsEq("l") { // "l" 伪条目承载副本分裂状态（写路径同一约定）
						fcmds = append(fcmds, kidb.Cmd{Name: "ZSCORE", Args: []any{keycodec.LexBucketKey(def.Name, idx.ID, slot, b), member}})
						checks = append(checks, fwdCheck{pk: row.pk, desc: idx.ID + "#lex=" + encVal})
					}
				}
			case meta.IndexRange:
				col, _ := def.Column(idx.Columns[0])
				score, err := rowcodec.ScoreOf(col.Type, encVal)
				if err != nil {
					continue
				}
				for _, b := range sh.ReadBucketsRange(score, score) {
					fcmds = append(fcmds, kidb.Cmd{Name: "ZSCORE", Args: []any{keycodec.RangeBucketKey(def.Name, idx.ID, slot, b), row.pk}})
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
		hit := map[string]bool{}      // pk|desc → 存在
		badScore := map[string]bool{} // pk|desc → score 不符
		for i, cres := range fres {
			if cres == nil {
				continue
			}
			c := checks[i]
			k := c.pk + "|" + c.desc
			hit[k] = true
			if c.checkScore {
				got, err := strconv.ParseFloat(fmt.Sprint(cres), 64)
				if err != nil || got != c.wantScore {
					badScore[k] = true
				}
			}
		}
		seen := map[string]bool{}
		for _, c := range checks {
			k := c.pk + "|" + c.desc
			if seen[k] {
				continue
			}
			seen[k] = true
			switch {
			case !hit[k]:
				r.drift("index_member_missing", def.Name, c.desc, c.pk)
			case badScore[k]:
				r.drift("index_score_mismatch", def.Name, c.desc, c.pk)
			}
		}
	}

	// 4. 反向：等值/唯一桶成员 → 行状态（死且已清扫 = 残留泄漏）。
	// 范围桶反向由正向的 score 校验覆盖（桶级反查边际收益低，不重复扇出）。
	var rcmds []kidb.Cmd
	var rchecks []struct{ covering bool }
	doneBucket := map[string]bool{}
	for _, row := range live {
		for j, idx := range indexes {
			if idx.Kind == meta.IndexRange {
				continue
			}
			encVal := row.vals[j]
			if encVal == "" {
				continue
			}
			sh, err := r.bm.Load(ctx, def.Name, idx.ID, slot)
			if err != nil {
				continue
			}
			for _, b := range sh.ReadBucketsEq(encVal) {
				bk := keycodec.EqBucketKey(def.Name, idx.ID, encVal, slot, b)
				if doneBucket[bk] {
					continue
				}
				doneBucket[bk] = true
				rcmds = append(rcmds, kidb.Cmd{Name: "ZRANGE", Args: []any{bk, 0, 63}})
				rchecks = append(rchecks, struct{ covering bool }{covering: len(idx.Covering) > 0})
			}
		}
	}
	if len(rcmds) > 0 {
		rres, err := r.cli.Pipeline(ctx, rcmds)
		if err != nil {
			return err
		}
		// 候选成员 pk（去重、排除活行）→ 直查行/回执活性：
		// 行死且回执已清 = 残留泄漏（回执在 = 清扫窗口合法暂态）。
		livePKs := map[string]bool{}
		for _, row := range live {
			livePKs[row.pk] = true
		}
		cand := map[string]string{} // pk → bucket key（日志描述）
		for i, cres := range rres {
			for _, member := range utils.Strings(cres) {
				pk := rowcodec.MemberPK(member, rchecks[i].covering)
				if livePKs[pk] || deadSwept[pk] {
					continue // 活行 / 已在取样面判定死且清扫
				}
				if _, ok := cand[pk]; !ok {
					cand[pk] = pksDesc(rcmds[i])
				}
			}
		}
		var ocmds []kidb.Cmd
		var opks []string
		for pk := range cand {
			ocmds = append(ocmds,
				kidb.Cmd{Name: "EXISTS", Args: []any{keycodec.RowKey(def.Name, pk)}},
				kidb.Cmd{Name: "EXISTS", Args: []any{keycodec.ReceiptKey(def.Name, pk)}})
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
	}

	// 5. 唯一预约残留巡检：数据侧推导预约 key → 占有者行活性
	// （预约在但占有行已死 = 自愈路径之外的长期泄漏）。
	var ucmds []kidb.Cmd
	var ukeys []string
	for _, row := range live {
		for j, idx := range indexes {
			if idx.Kind != meta.IndexUnique || row.vals[j] == "" {
				continue
			}
			uk := keycodec.UniqueKey(def.Name, idx.ID, row.vals[j])
			ucmds = append(ucmds, kidb.Cmd{Name: "GET", Args: []any{uk}})
			ukeys = append(ukeys, uk)
		}
	}
	if len(ucmds) > 0 {
		ures, err := r.cli.Pipeline(ctx, ucmds)
		if err != nil {
			return err
		}
		var ocmds []kidb.Cmd
		var okeys []string
		for i, ures1 := range ures {
			if ures1 == nil {
				continue // 无预约 = 无残留（缺失侧由正向检查覆盖）
			}
			owner := strings.SplitN(fmt.Sprint(ures1), "|", 2)[0] // 占有者行 key
			ocmds = append(ocmds, kidb.Cmd{Name: "EXISTS", Args: []any{owner}})
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

// drift 记录一次漂移（指标 + 告警；不自动修复——观测纪律）。
func (r *Reconciler) drift(kind, table, where, pk string) {
	if r.m != nil {
		r.m.ReconcileDrift.WithLabelValues(kind).Inc()
	}
	slog.Warn("kidb 对账漂移", "kind", kind, "table", table, "where", where, "pk", pk)
}

// hmgetStrings 归一 HMGET 回复（保留 NULL 语义：缺失字段 = ""，
// 不是 "<nil>"——utils.Strings 的 fmt.Sprint(nil) 形态不适用此处）。
func hmgetStrings(res any) (vals []string, allNil bool) {
	arr := utils.AnySlice(res)
	if len(arr) == 0 {
		return nil, true
	}
	vals = make([]string, len(arr))
	allNil = true
	for i, e := range arr {
		if e == nil {
			continue
		}
		allNil = false
		vals[i] = fmt.Sprint(e)
	}
	return vals, allNil
}

// pksDesc 调试描述（bucket key 原样透出）。
func pksDesc(c kidb.Cmd) string {
	if len(c.Args) > 0 {
		return fmt.Sprint(c.Args[0])
	}
	return c.Name
}
