package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"strconv"

	"golang.org/x/time/rate"

	"kidb"
	"kidb/bucketmap"
	"kidb/exec"
	"kidb/keycodec"
	"kidb/kv"
	"kidb/meta"
	"kidb/rowcodec"
	"kidb/script"
	"kidb/tuning"
	"kidb/txguard"
)

// jobrunner.go：DDL 后台作业执行器（docs/06 §6.3）：
// 作业持久于 Catalog `_job` 字段（游标断点续作）；Controller 巡检发现即接管——
// 无 owner 独占，表级 `_ver` CAS 防重，作业幂等。

// JobRunner 执行 DDL 作业。
type JobRunner struct {
	cli        kv.Client
	reg        *script.Registry
	store      *meta.CatalogStore
	cache      *meta.CatalogCache
	exec       *exec.Executor
	guard      *txguard.Guard
	bm         *bucketmap.Store
	slotsPerT  int           // 每批处理的 slot 数
	tickBudget time.Duration // 每 tick 回填时间预算
	bfLimit    *rate.Limiter // 回填/清理行速率（ddl_backfill_rate_limit 行/s/实例，docs/06 §6.3）
}

// NewJobRunner 构造（tickBudget 每 tick 回填时间预算，默认 500ms）。
// guard/reg 供 DROP 清理车道（SweepSlot + DeleteRow 走写路径语义）。
func NewJobRunner(cli kv.Client, reg *script.Registry, store *meta.CatalogStore, cache *meta.CatalogCache, e *exec.Executor, bm *bucketmap.Store, guard *txguard.Guard) *JobRunner {
	tn := tuning.Get()
	return &JobRunner{cli: cli, reg: reg, store: store, cache: cache, exec: e, bm: bm, guard: guard, slotsPerT: tn.Controller.JobSlotsPerTick, tickBudget: tn.JobTickBudget(),
		bfLimit: rate.NewLimiter(rate.Limit(tn.Controller.BackfillRowsPerSec), 1024)}
}

// SetBackfillRate 回填速率热更（行/s/实例；gateway 轮询 ddl_backfill_rate_limit 驱动）。
func (r *JobRunner) SetBackfillRate(rowsPerSec int) {
	if rowsPerSec <= 0 {
		return
	}
	r.bfLimit.SetLimit(rate.Limit(rowsPerSec))
}

// Tick 巡检一轮：所有表的进行中作业各推进一个批次。
func (r *JobRunner) Tick(ctx context.Context) error {
	tables, err := r.store.ListTables(ctx)
	if err != nil {
		return err
	}
	for _, name := range tables {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		job, err := r.store.GetJob(ctx, name)
		if err != nil || job == nil {
			continue
		}
		if err := r.step(ctx, name, job); err != nil {
			return err
		}
	}
	return r.tickDropJobs(ctx)
}

// step 推进作业：每个 tick 在时间预算内连跑多批（空/小表一轮即完成；
// 大表按批限速推进）。完成时转可见 + 清作业。
func (r *JobRunner) step(ctx context.Context, table string, job *meta.DDLJob) error {
	def, err := r.store.Load(ctx, table)
	if err != nil || def == nil {
		return err
	}
	switch job.Type {
	case "create_index":
		idx := job.Index
		if idx == nil {
			return r.finish(ctx, def)
		}
		deadline := time.Now().Add(r.tickBudget)
		for job.Cursor < keycodec.NumSlots {
			hi := min(job.Cursor+r.slotsPerT, keycodec.NumSlots)
			err := r.backfillSlots(ctx, def, idx, job.Cursor, hi)
			if errors.Is(err, errUniqueBackfillConflict) {
				// 回填期预约冲突 = 查重后与并发写入交错产生了真实重复
				// （MySQL 在线 DDL 同款收尾失败语义）——回滚索引定义，作业终止。
				return r.abortCreateIndex(ctx, def, job, err)
			}
			if err != nil {
				return err
			}
			job.Cursor = hi
			if time.Now().After(deadline) {
				break // 预算用尽，游标落库下轮续作
			}
		}
		if job.Cursor >= keycodec.NumSlots {
			return r.finish(ctx, def)
		}
		return r.store.SetJob(ctx, table, job) // 游标落库（断点）
	}
	return nil
}

// uniqueResv 一条待建的唯一预约（回填收集）。
type uniqueResv struct{ encVal, pk string }

// errUniqueBackfillConflict 唯一索引回填期预约冲突（存量/并发交错产生重复值）。
var errUniqueBackfillConflict = errors.New("kidb: unique backfill conflict")

// abortCreateIndex 建索引失败回滚：摘除 Building 索引定义 → 清作业 → 失效缓存。
// 已回填的桶成员不可见（索引已不在 Catalog）且经对账/后续 DROP 清理面覆盖。
func (r *JobRunner) abortCreateIndex(ctx context.Context, def *meta.TableDef, job *meta.DDLJob, cause error) error {
	fresh, err := r.store.Load(ctx, def.Name)
	if err != nil || fresh == nil {
		return err
	}
	kept := fresh.Indexes[:0]
	for _, i := range fresh.Indexes {
		if i.ID != job.Index.ID {
			kept = append(kept, i)
		}
	}
	fresh.Indexes = kept
	if err := r.store.Save(ctx, fresh, fresh.Ver); err != nil {
		return err
	}
	if err := r.store.ClearJob(ctx, def.Name); err != nil {
		return err
	}
	r.cache.Invalidate()
	slog.Warn("kidb 建唯一索引中止（存量/并发交错重复值），索引已回滚", "table", def.Name, "index", job.Index.ID, "cause", cause)
	if m := r.exec.Metrics(); m != nil {
		m.DDLJobDuration.WithLabelValues("create_index_abort").Observe(time.Since(time.Unix(job.Started, 0)).Seconds())
	}
	return nil
}

// finish 作业完成：索引转可见（Building=false）+ 清作业 + schema 版本递增。
func (r *JobRunner) finish(ctx context.Context, def *meta.TableDef) error {
	job, err := r.store.GetJob(ctx, def.Name)
	if err != nil {
		return err
	}
	if job != nil && job.Index != nil {
		for i := range def.Indexes {
			if def.Indexes[i].ID == job.Index.ID {
				def.Indexes[i].Building = false
			}
		}
		if err := r.store.Save(ctx, def, def.Ver); err != nil {
			return err
		}
	}
	if err := r.store.ClearJob(ctx, def.Name); err != nil {
		return err
	}
	r.cache.Invalidate()
	return nil
}

// backfillSlots 回填 [lo,hi) slot 区间（幂等 ZADD；并发写入双写由
// "索引先入 Catalog（Building）" 覆盖，docs/06 §6.3）。
func (r *JobRunner) backfillSlots(ctx context.Context, def *meta.TableDef, idx *meta.IndexDef, lo, hi int) error {
	col := idx.Columns[0]
	colDef, ok := def.Column(col)
	if !ok {
		return kidb.ErrContractViolation
	}
	s := r.exec.Run(ctx, &exec.Request{Table: def, Kind: exec.FullScan, SlotLo: lo, SlotHi: hi})
	defer func() { _ = s.Close() }() // 流收尾：错误无消费方，显式丢弃
	var cmds []kv.Cmd
	// 唯一索引回填建预约（review 实证缺失：不建预约 = 存量值对唯一约束不可见）。
	// 先与 ZADD 同 pipeline SET NX；NX 失败的行经预约自愈复查，仍冲突 = 真实重复 → 中止作业。
	var resv []uniqueResv
	rowsInFlight := 0
	flush := func() error {
		if len(cmds) == 0 {
			return nil
		}
		// 回填限速（docs/06 §6.3：ddl_backfill_rate_limit 行/s/实例，
		// 对在线读写的影响有上限）——按行计额度，ctx 取消贯穿
		if err := r.bfLimit.WaitN(ctx, rowsInFlight); err != nil {
			return err
		}
		if _, err := r.cli.Pipeline(ctx, cmds); err != nil {
			return err
		}
		cmds = cmds[:0]
		rowsInFlight = 0
		if len(resv) > 0 {
			if err := r.reserveBatch(ctx, def, idx, resv); err != nil {
				return err
			}
			resv = resv[:0]
		}
		return nil
	}
	for {
		row, err := s.Next()
		if err != nil {
			break
		}
		var pk string
		var ci int
		coverIdx := make([]int, 0, len(idx.Covering))
		for i, c := range def.Columns {
			if c.Name == def.PK {
				pk = sprintOf(row[i])
			}
			if c.Name == col {
				ci = i
			}
			for _, cc := range idx.Covering {
				if c.Name == cc {
					coverIdx = append(coverIdx, i)
				}
			}
		}
		if row[ci] == nil {
			continue
		}
		enc, err := rowcodec.Encode(colDef.Type, row[ci])
		if err != nil {
			return err
		}
		// 覆盖列随 member 编码（docs/03 §3.5；漏写会让覆盖读路径全灭回退）
		var covers []string
		for _, ci2 := range coverIdx {
			cv := ""
			if row[ci2] != nil {
				ct, _ := def.Column(def.Columns[ci2].Name)
				cv, err = rowcodec.Encode(ct.Type, row[ci2])
				if err != nil {
					return err
				}
			}
			covers = append(covers, cv)
		}
		member := rowcodec.EncodeMember(pk, covers)
		slot := keycodec.Slot(keycodec.RowKey(def.Name, pk))
		rowsInFlight++
		if idx.Kind == meta.IndexUnique {
			resv = append(resv, uniqueResv{encVal: enc, pk: pk})
		}
		switch idx.Kind {
		case meta.IndexRange:
			score, err := rowcodec.ScoreOf(colDef.Type, enc)
			if err != nil {
				return err
			}
			cmds = append(cmds, kv.Cmd{Name: "ZADD", Args: []any{keycodec.RangeBucketKey(def.Name, idx.ID, slot, 0), score, member}})
		default:
			cmds = append(cmds, kv.Cmd{Name: "ZADD", Args: []any{keycodec.EqBucketKey(def.Name, idx.ID, enc, slot, 0), 0, member}})
		}
		// 字典序副本随等值索引回填（docs/04 §4.5 前缀搜索的数据面；
		// 与 txguard 写路径同一编码——rowcodec.LexMember 单点）
		if idx.PrefixCopy && idx.Kind != meta.IndexRange {
			cmds = append(cmds, kv.Cmd{Name: "ZADD", Args: []any{keycodec.LexBucketKey(def.Name, idx.ID, slot, 0), 0, rowcodec.LexMember(enc, pk)}})
		}
		// 基数统计采样（docs/04 §4.6：与增量写入同一按值采样规则）
		if txguard.HLLSampledValue(idx.ID, enc) {
			cmds = append(cmds, kv.Cmd{Name: "PFADD", Args: []any{keycodec.HLLKey(def.Name, idx.ID), enc}})
		}
		if len(cmds) >= 512 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func sprintOf(v any) string { return fmt.Sprint(v) }

// reserveBatch 唯一索引回填的预约批：SET NX 管线，失败行经自愈复查，
// 仍冲突 = 存量/并发交错真实重复 → errUniqueBackfillConflict（调用方中止作业）。
func (r *JobRunner) reserveBatch(ctx context.Context, def *meta.TableDef, idx *meta.IndexDef, resv []uniqueResv) error {
	cmds := make([]kv.Cmd, 0, len(resv))
	for _, rv := range resv {
		rk := keycodec.RowKey(def.Name, rv.pk)
		cmds = append(cmds, kv.Cmd{Name: "SET", Args: []any{
			keycodec.UniqueKey(def.Name, idx.ID, rv.encVal),
			rk + "|" + strconv.FormatInt(time.Now().Unix(), 10), "NX",
		}})
	}
	results, err := r.cli.Pipeline(ctx, cmds)
	if err != nil {
		return err
	}
	for i, res := range results {
		if res != nil && fmt.Sprint(res) == "OK" {
			continue
		}
		// NX 失败：占有者活检查 + 自愈（与写路径同一纪律，txguard 导出方法）
		ok, err := r.guard.ReserveUniqueForBackfill(ctx, def, idx.ID, resv[i].encVal, resv[i].pk)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: %s on %s (rows ...%s)", errUniqueBackfillConflict, resv[i].encVal, idx.ID, resv[i].pk)
		}
	}
	return nil
}
