package controller

import (
	"context"
	"fmt"
	"time"

	"kidb/keycodec"
	"kidb/kv"
	"kidb/meta"
	"kidb/sweeper"
	"kidb/tuning"
	"kidb/utils"
)

// dropjob.go：DROP TABLE 大表后台清理作业车道（docs/06 §6.3）。
//
// 语义：Catalog 在 DROP 时即删（表即刻不可查询），数据清理由本车道按 slot
// 游标断点续作——每 slot 先 SweepSlot 清过期残留（孤儿成员/回执/预约释放），
// 再分页删活行（bfLimit 限速与回填共享车道）；完成时清残留 key
// （exp 登记册/bm 分片/HLL/异步日志/自增序列）+ 注销作业。
//
// 崩溃安全：作业先登记、Catalog 后删（DROP 路径顺序）——Catalog 仍在的作业
// 说明 DROP 在途，本车道跳过（幂等，下一轮重判）。

// tickDropJobs 巡检 DROP 清理作业（Tick 末段调用）。
func (r *JobRunner) tickDropJobs(ctx context.Context) error {
	jobs, err := r.store.ListDropJobs(ctx)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// DROP 在途防护（Catalog 未删 = DROP 未生效，跳过本轮）
		def, err := r.store.Load(ctx, job.Table)
		if err != nil {
			continue
		}
		if def != nil {
			continue
		}
		if err := r.stepDrop(ctx, job); err != nil {
			return err
		}
	}
	return nil
}

// stepDrop 推进一个作业批次：游标起 slotsPerT 个 slot，每 slot 清扫+删行。
func (r *JobRunner) stepDrop(ctx context.Context, job *meta.DropJob) error {
	def := job.Def
	sw := sweeper.New(r.cli, r.reg)
	batch := tuning.Get().Exec.Batch

	hi := job.Cursor + r.slotsPerT
	if hi > keycodec.NumSlots {
		hi = keycodec.NumSlots
	}
	for slot := job.Cursor; slot < hi; slot++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// 1. 过期残留清扫（孤儿成员/回执/唯一预约释放）
		if _, err := sw.SweepSlot(ctx, def, uint16(slot)); err != nil {
			return fmt.Errorf("dropjob %s slot %d sweep: %w", job.Table, slot, err)
		}
		// 2. 登记册分页清空（恒读首页：处理即移除，终止由构造保证）：
		// 活行 DeleteRow（写路径全清：行/索引/登记册/回执/预约）；
		// 死行 DeleteRow 是 no-op（write_row D 分支不撤 exp）——
		// 收集后立即强制清扫出登记册（SweepPksForced：按回执清桶成员 +
		// 预约释放），否则分页循环在死行上空转（实现期实证死锁形态）。
		expKey := keycodec.ExpKeyN(def.Name, uint16(slot), 0, def.EffectiveExpShards())
		for shard := 0; shard < def.EffectiveExpShards(); shard++ {
			expKey = keycodec.ExpKeyN(def.Name, uint16(slot), shard, def.EffectiveExpShards())
			for {
				res, err := r.cli.Do(ctx, "ZRANGE", expKey, 0, batch-1)
				if err != nil {
					return err
				}
				pks := utils.Strings(res)
				if len(pks) == 0 {
					break
				}
				var dead []string
				for _, pk := range pks {
					if err := r.bfLimit.Wait(ctx); err != nil {
						return err
					}
					deleted, err := r.guard.DeleteRow(ctx, def, pk)
					if err != nil {
						return err
					}
					if !deleted {
						dead = append(dead, pk)
					}
				}
				if _, err := sw.SweepPksForced(ctx, def, uint16(slot), shard, dead); err != nil {
					return err
				}
			}
		}
		job.Cursor = slot + 1
	}
	if err := r.store.SetDropJob(ctx, job); err != nil { // 游标落库（断点）
		return err
	}
	if job.Cursor >= keycodec.NumSlots {
		return r.finishDrop(ctx, job)
	}
	return nil
}

// finishDrop 清理残留 key 并注销作业（一次性、有界、异步）。
func (r *JobRunner) finishDrop(ctx context.Context, job *meta.DropJob) error {
	def := job.Def
	var cmds []kv.Cmd
	flush := func() error {
		if len(cmds) == 0 {
			return nil
		}
		_, err := r.cli.Pipeline(ctx, cmds)
		cmds = cmds[:0]
		return err
	}
	push := func(name string, args ...any) error {
		cmds = append(cmds, kv.Cmd{Name: name, Args: args})
		if len(cmds) >= 512 {
			return flush()
		}
		return nil
	}

	// exp 登记册（应为空，防御性 UNLINK）+ bm 分片（稀疏，盲删有界一次性）
	for slot := 0; slot < keycodec.NumSlots; slot++ {
		if err := push("UNLINK", keycodec.ExpKeyN(def.Name, uint16(slot), 0, def.EffectiveExpShards())); err != nil {
			return err
		}
		for i := range def.Indexes {
			idx := &def.Indexes[i]
			if err := push("UNLINK", keycodec.BucketMapSlotKey(def.Name, idx.ID, uint16(slot))); err != nil {
				return err
			}
			if idx.Async {
				if err := push("UNLINK", keycodec.AsyncLogKey(def.Name, idx.ID, uint16(slot))); err != nil {
					return err
				}
			}
		}
	}
	// 索引级残留：HLL 基数 + 热值注册表
	for i := range def.Indexes {
		idx := &def.Indexes[i]
		if err := push("UNLINK", keycodec.HLLKey(def.Name, idx.ID)); err != nil {
			return err
		}
		if err := push("UNLINK", keycodec.BucketMapHotKey(def.Name, idx.ID)); err != nil {
			return err
		}
	}
	// 自增序列
	if def.AutoIncrColumn != "" {
		if err := push("UNLINK", keycodec.SeqKey(def.Name)); err != nil {
			return err
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if err := r.store.ClearDropJob(ctx, job.Table); err != nil {
		return err
	}
	if m := r.exec.Metrics(); m != nil {
		m.DDLJobDuration.WithLabelValues("drop_table").Observe(time.Since(time.Unix(job.Started, 0)).Seconds())
	}
	return nil
}
