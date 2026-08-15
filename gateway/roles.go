package gateway

import (
	"context"
	"fmt"
	"time"

	"kidb/bucketmap"
	"kidb/controller"
	"kidb/indexer"
	"kidb/engine"
	"kidb/internal/tuning"
	"kidb/keycodec"
	"kidb/nearcache"
	"kidb/sweeper"
	"kidb/telemetry"
)

// roles.go：后台角色的装配与驱动（docs/08 §8.5）：
// 所有节点默认参与；ReadWriteOnly 节点经 Bootstrap 显式豁免。
// 锁即选举 + watchdog 续约；全部角色故障安全（全挂只会变慢，不出错行）。

// 周期/区间取自 tuning.toml（sweeper.tick_ms / sweep_range_slots；indexer 100ms）。
var (
	sweepTick   = time.Duration(tuning.Get().Sweeper.TickMs) * time.Millisecond
	indexerTick = 100 * time.Millisecond
	sweepRange  = tuning.Get().Sweeper.SweepRangeSlots
)

// startRoles 启动后台角色循环（ReadWriteOnly 豁免）。
func (s *Server) startRoles(ctx context.Context) {
	if s.roleCancel != nil {
		return
	}
	roleCtx, cancel := context.WithCancel(ctx)
	s.roleCancel = cancel
	s.elector = controller.CtrlLock(s.deps.Client, s.deps.Reg, fmt.Sprintf("kidb@%d", time.Now().UnixNano()))

	// 自治链路：遥测采样 → 候选登记 → Controller 复核分裂/L4
	rec := telemetry.New(s.deps.Client)
	s.deps.Exec.SetTelemetry(rec)
	bm := bucketmap.New(s.deps.Client, s.deps.Reg)
	s.deps.Exec.SetBucketMap(bm)
	spl := controller.NewSplitter(s.deps.Client, s.deps.Reg, bm)
	l4 := controller.NewL4(s.deps.Client, s.deps.Reg)
	s.deps.Exec.SetL4(l4)
	s.manager = controller.NewManager(s.deps.Client, bm, spl, l4)
	s.jobrunner = controller.NewJobRunner(s.deps.Client, s.deps.Store, s.deps.Cache, s.deps.Exec, bm)

	// 读路径开关装配（docs/01 §1.0：变量只承载语义开关——replica_read /
	// hotkey_row_cache；L1/L2 常量装配一次到位：3s/10000，调优参数不设变量）。
	s.deps.Exec.SetNearCache(nearcache.NewSharded[[]string](tuning.Get().Nearcache.Capacity, tuning.Get().NearcacheTTL()))
	s.attachSemanticSwitches(roleCtx)

	go s.elector.Campaign(roleCtx, s.controllerRole)
	go s.sweeperLoop(roleCtx)
	go s.indexerLoop(roleCtx)
}

// attachSemanticSwitches 语义开关轮询（1s，与 schema lease 同节奏）：
// L3 副本读（replica_read × 适配器能力位）与行级近缓存（hotkey_row_cache，
// 默认关闭——陈旧窗口语义见 docs/08 §8.4）。容量/TTL 用内置常量（3s/10000）。
// 开关值读本实例 gms 注册表（单一事实源，docs/02 §2.7）。
func (s *Server) attachSemanticSwitches(ctx context.Context) {
	var curRow *nearcache.RowCache
	apply := func() {
		// L3：能力位缺失时变量无效（自动降级，docs/09 §9.4）
		on := s.deps.Client.Capabilities().ReplicaRead && engine.SysvarBool(engine.VarReplicaRead)
		s.deps.Exec.SetReplicaRead(on)
		// 行级近缓存（默认关闭）
		if engine.SysvarBool(engine.VarRowCache) {
			if curRow == nil {
				curRow = nearcache.NewRowCache(tuning.Get().Nearcache.RowCapacity, tuning.Get().NearcacheRowTTL())
				s.deps.Exec.SetRowCache(curRow)
			}
		} else if curRow != nil {
			s.deps.Exec.SetRowCache(nil)
			_ = curRow.Close()
			curRow = nil
		}
	}
	apply()
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				if curRow != nil {
					_ = curRow.Close()
				}
				return
			case <-t.C:
				apply()
			}
		}
	}()
}

// controllerRole 控制循环：遥测候选复核 → 分裂/L4 决策（docs/08 §8.1/§8.2）。
// 仅锁持有者干活；失约由 watchdog 立即 cancel（docs/08 §8.5）。
func (s *Server) controllerRole(ctx context.Context) error {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			_ = s.manager.Tick(ctx)   // 错误不致命：下轮再来（故障安全）
			_ = s.jobrunner.Tick(ctx) // DDL 作业巡检（docs/06 §6.3）
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
