package gateway

import (
	"context"
	"fmt"
	"time"

	"kidb"
	"kidb/bucketmap"
	"kidb/controller"
	"kidb/engine"
	"kidb/exec"
	"kidb/indexer"
	"kidb/keycodec"
	"kidb/meta"
	"kidb/nearcache"
	"kidb/script"
	"kidb/sweeper"
	"kidb/tuning"
	"kidb/utils"
)

// roles.go：后台角色（docs/08 §8.5）的组件装配与循环驱动。
// 所有节点默认参与；ReadWriteOnly 节点豁免（DI 层不构造 Roles）。
// 锁即选举 + watchdog 续约；全部角色故障安全（全挂只会变慢，不出错行）。

// Roles 后台角色组件集（DI 图节点：wire provider 与测试共用 AssembleRoles）。
type Roles struct {
	Elector    *controller.Elector
	Manager    *controller.Manager
	JobRunner  *controller.JobRunner
	Reconciler *controller.Reconciler
	Sweeper    *sweeper.Sweeper
	Indexer    *indexer.Indexer
}

// AssembleRoles 后台角色组件的唯一构造函数（单一装配点纪律）：
// 自治链路 = 遥测采样 → 候选登记 → Controller 复核分裂/L4 → DDL 作业巡检。
// 读路径附件（telemetry/bm/l4/L1 近缓存）的 executor 接线在 DI executor
// provider 完成（di.ProvideExecutor），不在本函数。
func AssembleRoles(cli kidb.KvClient, reg *script.Registry, store *meta.CatalogStore, cache *meta.CatalogCache, ex *exec.Executor, bm *bucketmap.Store) *Roles {
	return &Roles{
		Elector:    controller.CtrlLock(cli, reg, fmt.Sprintf("kidb@%d", time.Now().UnixNano())),
		Manager:    controller.NewManager(cli, bm, controller.NewSplitter(cli, reg, bm), controller.NewL4(cli, reg)),
		JobRunner:  controller.NewJobRunner(cli, store, cache, ex, bm),
		Reconciler: controller.NewReconciler(cli, store, bm, ex.Metrics()),
		Sweeper:    sweeper.New(cli, reg),
		Indexer:    indexer.New(cli),
	}
}

// 周期/区间取自 tuning.toml（sweeper.tick_ms / sweep_range_slots；indexer 100ms）。
var (
	sweepTick   = time.Duration(tuning.Get().Sweeper.TickMs) * time.Millisecond
	indexerTick = 100 * time.Millisecond
	sweepRange  = tuning.Get().Sweeper.SweepRangeSlots
)

// startRoles 启动后台角色循环。
func (s *Server) startRoles(ctx context.Context) {
	if s.roleCancel != nil {
		return
	}
	roleCtx, cancel := context.WithCancel(ctx)
	s.roleCancel = cancel

	// 语义开关轮询装配（L3 副本读 / 行级近缓存；值读 gms 注册表）
	s.attachSemanticSwitches(roleCtx)

	go s.roles.Elector.Campaign(roleCtx, s.controllerRole)
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
			_ = s.roles.Manager.Tick(ctx)    // 错误不致命：下轮再来（故障安全）
			_ = s.roles.JobRunner.Tick(ctx)  // DDL 作业巡检（docs/06 §6.3）
			_ = s.roles.Reconciler.Tick(ctx) // 抽样对账（docs/12 §12.8；只观测不修复）
		}
	}
}

// sweeperLoop 过期清扫：slot 区间分摊（锁隔离），逐区间驱动。
func (s *Server) sweeperLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		tables, err := s.deps.Store.ListTables(ctx)
		if err != nil {
			_ = utils.SleepCtx(ctx, sweepTick)
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
					if _, err := s.roles.Sweeper.SweepSlot(ctx, def, uint16(slot)); err != nil {
						break // 本区间出错（如节点宕机）→ 下区间继续，不堵全局
					}
				}
			}
		}
		_ = utils.SleepCtx(ctx, sweepTick) // 空闲降频由"一轮有产出则立即再来"后续精化
	}
}

// indexerLoop 异步索引日志消费（无锁：LRANGE+LTRIM 并发重复消费由 ZSet 幂等吸收）。
func (s *Server) indexerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		tables, err := s.deps.Store.ListTables(ctx)
		if err != nil {
			_ = utils.SleepCtx(ctx, indexerTick)
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
					if _, err := s.roles.Indexer.ConsumeLog(ctx, def, idx, uint16(slot)); err != nil {
						break
					}
				}
			}
		}
		_ = utils.SleepCtx(ctx, indexerTick)
	}
}
