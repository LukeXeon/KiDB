package controller

import (
	"context"
	"fmt"

	"kidb"
	"kidb/bucketmap"
	"kidb/exec"
	"kidb/keycodec"
	"kidb/meta"
	"kidb/rowcodec"
)

// jobrunner.go：DDL 后台作业执行器（docs/06 §6.3）：
// 作业持久于 Catalog `_job` 字段（游标断点续作）；Controller 巡检发现即接管——
// 无 owner 独占，表级 `_ver` CAS 防重，作业幂等。

// JobRunner 执行 DDL 作业。
type JobRunner struct {
	cli       kidb.Store
	store     *meta.CatalogStore
	cache     *meta.CatalogCache
	exec      *exec.Executor
	bm        *bucketmap.Store
	slotsPerT int // 每 tick 处理的 slot 数（回填限速由批大小保证）
}

// NewJobRunner 构造。
func NewJobRunner(cli kidb.Store, store *meta.CatalogStore, cache *meta.CatalogCache, e *exec.Executor, bm *bucketmap.Store) *JobRunner {
	return &JobRunner{cli: cli, store: store, cache: cache, exec: e, bm: bm, slotsPerT: 256}
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
	return nil
}

// step 推进一个作业批次；完成时转可见 + 清作业。
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
		// 回填一批 slot（游标续作）
		hi := job.Cursor + r.slotsPerT
		if hi > keycodec.NumSlots {
			hi = keycodec.NumSlots
		}
		if err := r.backfillSlots(ctx, def, idx, job.Cursor, hi); err != nil {
			return err
		}
		job.Cursor = hi
		if hi >= keycodec.NumSlots {
			return r.finish(ctx, def)
		}
		return r.store.SetJob(ctx, table, job) // 游标落库（断点）
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
	defer s.Close()
	var cmds []kidb.Cmd
	flush := func() error {
		if len(cmds) == 0 {
			return nil
		}
		_, err := r.cli.Pipeline(ctx, cmds)
		cmds = cmds[:0]
		return err
	}
	for {
		row, err := s.Next()
		if err != nil {
			break
		}
		var pk string
		var ci int
		for i, c := range def.Columns {
			if c.Name == def.PK {
				pk = sprintOf(row[i])
			}
			if c.Name == col {
				ci = i
			}
		}
		if row[ci] == nil {
			continue
		}
		enc, err := rowcodec.Encode(colDef.Type, row[ci])
		if err != nil {
			return err
		}
		slot := keycodec.Slot(keycodec.RowKey(def.Name, pk))
		switch idx.Kind {
		case meta.IndexRange:
			score, err := rowcodec.ScoreOf(colDef.Type, enc)
			if err != nil {
				return err
			}
			cmds = append(cmds, kidb.Cmd{Name: "ZADD", Args: []any{keycodec.RangeBucketKey(def.Name, idx.ID, slot, 0), score, pk}})
		default:
			cmds = append(cmds, kidb.Cmd{Name: "ZADD", Args: []any{keycodec.EqBucketKey(def.Name, idx.ID, enc, slot, 0), 0, pk}})
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
