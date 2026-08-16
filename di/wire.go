//go:build wireinject

//go:generate wire

package di

import (
	"github.com/google/wire"

	"kidb"
	"kidb/gateway"
)

// InitializeServer 生产装配的唯一入口（wire 编译期生成 wire_gen.go，
// 入库；改动 provider 后 `go generate ./di` 重新生成）。
func InitializeServer(boot kidb.Bootstrap) (*gateway.Server, error) {
	wire.Build(
		ProvideClient,
		ProvideKernel,
		ProvideScripts,
		ProvideMetrics,
		ProvideSyncClock,
		ProvideCatalogStore,
		ProvideCatalogCache,
		ProvideBucketMap,
		ProvideTelemetry,
		ProvideL4,
		ProvideNearCache,
		ProvideExecutor,
		ProvideGuard,
		ProvideConfigStore,
		ProvideEngineDeps,
		ProvideRoles,
		gateway.NewServer,
	)
	return nil, nil
}
