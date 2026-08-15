package di

import (
	"testing"

	"github.com/stretchr/testify/require"

	"kidb"
	"kidb/testutil"
)

// TestProvideRolesRWOnly ReadWriteOnly 豁免后台角色（docs/08 §8.5）；
// 默认参与。DI 图的语义分支钉死。
func TestProvideRolesRWOnly(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	ex := ProvideExecutor(cli, reg, ProvideMetrics(), ProvideTelemetry(cli), ProvideBucketMap(cli, reg), ProvideL4(cli, reg), ProvideNearCache())
	require.NotNil(t, ex)

	store := ProvideCatalogStore(cli, reg)
	cache := ProvideCatalogCache(store)
	bm := ProvideBucketMap(cli, reg)

	roles := ProvideRoles(kidb.Bootstrap{}, cli, reg, store, cache, ex, bm)
	require.NotNil(t, roles)
	require.NotNil(t, roles.Elector)
	require.NotNil(t, roles.Manager)
	require.NotNil(t, roles.JobRunner)
	require.NotNil(t, roles.Sweeper)
	require.NotNil(t, roles.Indexer)

	rwOnly := ProvideRoles(kidb.Bootstrap{ReadWriteOnly: true}, cli, reg, store, cache, ex, bm)
	require.Nil(t, rwOnly)
}

// TestProvideEngineDepsGate 引擎依赖面必带全扫闸门（装配缺口即事故，钉死）。
func TestProvideEngineDepsGate(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	store := ProvideCatalogStore(cli, reg)
	deps := ProvideEngineDeps(cli, reg, store, ProvideCatalogCache(store),
		ProvideExecutor(cli, reg, ProvideMetrics(), ProvideTelemetry(cli), ProvideBucketMap(cli, reg), ProvideL4(cli, reg), ProvideNearCache()),
		ProvideGuard(cli, reg, ProvideBucketMap(cli, reg)), ProvideMetrics())
	require.NotNil(t, deps.FullscanGate)
}
