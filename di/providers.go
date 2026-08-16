// Package di 是 KiDB 的依赖注入装配面（v6.0，docs/01 §1.6）：
// 全项目组件经 google/wire 编译期注入装配，wire_gen.go 入库（CI 校验重新
// 生成一致）。DI 图是生产装配的唯一入口——"装配缺口"类事故（开关只在测试
// 接线、生产休眠）由图的显式性根除。
//
// 纪律：provider 只做构造与接线，不藏逻辑；组件自身的运行循环（后台角色
// goroutine、配置轮询）仍由组件拥有（gateway.Server 启动）。
package di

import (
	"kidb"
	"kidb/adapter/goredis"
	"kidb/bucketmap"
	"kidb/config"
	"kidb/controller"
	"kidb/engine"
	"kidb/exec"
	"kidb/gateway"
	"kidb/meta"
	"kidb/metrics"
	"kidb/nearcache"
	"kidb/script"
	"kidb/telemetry"
	"kidb/tuning"
	"kidb/txguard"
)

// ProvideClient 构造参考适配器并包退避矩阵（docs/09 §9.6：MOVED/CLUSTERDOWN/
// LOADING/READONLY/TRYAGAIN/超时按类分派退避，耗尽映射哨兵错误）——
// 装饰在契约面上，全部消费方零感知。
func ProvideClient(boot kidb.Bootstrap) kidb.KvClient {
	return kidb.NewRetryingClient(goredis.New(boot.Addrs, goredis.Options{
		PoolSize:     boot.PoolSize,
		ReadTimeout:  boot.ReadTimeout,
		WriteTimeout: boot.WriteTimeout,
		ReplicaRead:  boot.ReplicaRead,
	}), kidb.DefaultRetryPolicy())
}

// ProvideKernel 内核组装（Lua 资产加载 + 能力探测，docs/09 §9.4）。
// wire 不支持变参，包一层 NewKernel 的 opts。
func ProvideKernel(cli kidb.KvClient, boot kidb.Bootstrap) (*kidb.Kernel, error) {
	return kidb.NewKernel(cli, boot)
}

// ProvideScripts Lua 资产注册表（Kernel 完成加载 + 能力探测后取出）。
func ProvideScripts(k *kidb.Kernel) *script.Registry { return k.Scripts() }

// ProvideMetrics 指标体系（默认 prometheus registry，docs/10 §10.3）。
func ProvideMetrics() *metrics.Metrics { return metrics.New(nil) }

// ProvideCatalogStore Catalog 存储（docs/06）。
func ProvideCatalogStore(cli kidb.KvClient, reg *script.Registry) *meta.CatalogStore {
	return meta.NewCatalogStore(cli, reg)
}

// ProvideCatalogCache Catalog 本地缓存（schema lease，docs/06 §6.2）。
func ProvideCatalogCache(store *meta.CatalogStore) *meta.CatalogCache {
	return meta.NewCatalogCache(store)
}

// ProvideBucketMap BucketMap 存储（docs/06）。
func ProvideBucketMap(cli kidb.KvClient, reg *script.Registry) *bucketmap.Store {
	return bucketmap.New(cli, reg)
}

// ProvideTelemetry 遥测采样器（自治链路入口，docs/08 §8.1）。
func ProvideTelemetry(cli kidb.KvClient) *telemetry.Recorder {
	return telemetry.New(cli)
}

// ProvideL4 热桶副本管理器（docs/08 §8.4）。
func ProvideL4(cli kidb.KvClient, reg *script.Registry) *controller.L4Manager {
	return controller.NewL4(cli, reg)
}

// ProvideNearCache L1 谓词结果近缓存（内置常量装配：3s/1 万条，docs/08 §8.4）。
func ProvideNearCache() *nearcache.ShardedCache[[]string] {
	return nearcache.NewSharded[[]string](tuning.Get().Nearcache.Capacity, tuning.Get().NearcacheTTL())
}

// ProvideExecutor 查询执行器——读路径附件全部在此接线（单一装配点：
// 指标/遥测/BucketMap/L4/L1 近缓存；运行时开关项 replica_read/行缓存
// 由 gateway 语义开关轮询驱动，docs/02 §2.7）。
func ProvideExecutor(cli kidb.KvClient, reg *script.Registry, m *metrics.Metrics,
	tel *telemetry.Recorder, bm *bucketmap.Store, l4 *controller.L4Manager,
	nc *nearcache.ShardedCache[[]string]) *exec.Executor {
	ex := exec.New(cli, reg)
	ex.SetMetrics(m)
	ex.SetTelemetry(tel)
	ex.SetBucketMap(bm)
	ex.SetL4(l4)
	ex.SetNearCache(nc)
	return ex
}

// ProvideGuard 写入事务卫护（单 slot Lua 编排，docs/05）。
func ProvideGuard(cli kidb.KvClient, reg *script.Registry, bm *bucketmap.Store) *txguard.Guard {
	return txguard.New(cli, reg, bm)
}

// ProvideConfigStore 配置存储（cfg:global，docs/10 §10.2）。
func ProvideConfigStore(cli kidb.KvClient, reg *script.Registry) *config.Store {
	return config.New(cli, reg, gateway.ConfigActor)
}

// ProvideEngineDeps 引擎依赖面 + 全扫闸门（docs/07 §7.4）。
func ProvideEngineDeps(cli kidb.KvClient, reg *script.Registry, store *meta.CatalogStore,
	cache *meta.CatalogCache, ex *exec.Executor, guard *txguard.Guard, m *metrics.Metrics) engine.Deps {
	return engine.Deps{
		Client:       cli,
		Reg:          reg,
		Store:        store,
		Cache:        cache,
		Exec:         ex,
		Guard:        guard,
		FullscanGate: engine.NewFullscanGate(m),
	}
}

// ProvideRoles 后台角色组件（ReadWriteOnly 节点豁免 = nil，docs/08 §8.5）。
func ProvideRoles(boot kidb.Bootstrap, cli kidb.KvClient, reg *script.Registry,
	store *meta.CatalogStore, cache *meta.CatalogCache, ex *exec.Executor, bm *bucketmap.Store,
	guard *txguard.Guard) *gateway.Roles {
	if boot.ReadWriteOnly {
		return nil
	}
	return gateway.AssembleRoles(cli, reg, store, cache, ex, bm, guard)
}
