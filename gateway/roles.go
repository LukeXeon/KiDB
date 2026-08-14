package gateway

import (
	"context"
	"fmt"
	"time"

	"kidb/controller"
	"kidb/indexer"
	"kidb/keycodec"
	"kidb/sweeper"
)

// roles.go：后台角色的装配与驱动（docs/08 §8.5）：
// 所有节点默认参与；ReadWriteOnly 节点经 Bootstrap 显式豁免。
// 锁即选举 + watchdog 续约；全部角色故障安全（全挂只会变慢，不出错行）。

const (
	sweepTick   = time.Second            // docs/07 §7.3
	indexerTick = 100 * time.Millisecond // docs/05 §5.2 秒级收敛
	sweepRange  = 1024                   // slot 区间宽度（lk:sweep:{区间} 锁粒度）
)

// startRoles 启动后台角色循环（ReadWriteOnly 豁免）。
func (s *Server) startRoles(ctx context.Context) {
	if s.roleCancel != nil {
		return
	}
	roleCtx, cancel := context.WithCancel(ctx)
	s.roleCancel = cancel
	s.elector = controller.CtrlLock(s.deps.Client, s.deps.Reg, fmt.Sprintf("kidb@%d", time.Now().UnixNano()))

	go s.elector.Campaign(roleCtx, s.controllerRole)
	go s.sweeperLoop(roleCtx)
	go s.indexerLoop(roleCtx)
}

// controllerRole 控制循环（分裂/合并 + DDL 作业巡检随下一批落地桶状态机后接活；
// 当前为保活的占位 tick——选举/故障迁移语义本身已生效）。
func (s *Server) controllerRole(ctx context.Context) error {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			// TODO(impl)：桶分裂/合并决策 + DDL 作业巡检（docs/08 §8.2/§8.3、docs/06 §6.3）
		}
	}
}

// sweeperLoop 过期清扫：slot 区间分摊（锁隔离），逐区间驱动。
func (s *Server) sweeperLoop(ctx context.Context) {
	sw := sweeper.New(s.deps.Client, s.deps.Reg)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		tables, err := s.deps.Store.ListTables(ctx)
		if err != nil {
			sleepCtx(ctx, sweepTick)
			continue
		}
		for _, name := range tables {
			def, err := s.deps.Cache.Get(ctx, name)
			if err != nil || def == nil {
				continue
			}
			for start := 0; start < keycodec.NumSlots; start += sweepRange {
				if ctx.Err() != nil {
					return
				}
				// 区间锁（docs/07 §7.3：宕机自动重分配由锁到期保证；
				// 多副本竞争同区间时只有一个持有者干活，重复清扫幂等安全）
				lockKey := keycodec.SweepLockKey(uint16(start), uint16(start+sweepRange-1))
				got, err := s.deps.Client.Do(ctx, "SET", lockKey, "sweeper", "PX", int64(2*sweepTick/time.Millisecond), "NX")
				if err != nil || got == nil {
					continue
				}
				for slot := start; slot < start+sweepRange; slot++ {
					if _, err := sw.SweepSlot(ctx, def, uint16(slot)); err != nil {
						break // 本区间出错（如节点宕机）→ 下区间继续，不堵全局
					}
				}
			}
		}
		sleepCtx(ctx, sweepTick) // 空闲降频由"一轮有产出则立即再来"后续精化
	}
}

// indexerLoop 异步索引日志消费（无锁：LRANGE+LTRIM 并发重复消费由 ZSet 幂等吸收）。
func (s *Server) indexerLoop(ctx context.Context) {
	ix := indexer.New(s.deps.Client)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		tables, err := s.deps.Store.ListTables(ctx)
		if err != nil {
			sleepCtx(ctx, indexerTick)
			continue
		}
		for _, name := range tables {
			def, err := s.deps.Cache.Get(ctx, name)
			if err != nil || def == nil {
				continue
			}
			for i := range def.Indexes {
				idx := &def.Indexes[i]
				if !idx.Async {
					continue
				}
				for slot := 0; slot < keycodec.NumSlots; slot++ {
					if ctx.Err() != nil {
						return
					}
					if _, err := ix.ConsumeLog(ctx, def, idx, uint16(slot)); err != nil {
						break
					}
				}
			}
		}
		sleepCtx(ctx, indexerTick)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
