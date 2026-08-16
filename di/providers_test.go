package di

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"kidb"
	"kidb/metrics"
	"kidb/testutil"
)

// TestProvideRolesRWOnly ReadWriteOnly 豁免后台角色（docs/08 §8.5）；
// 默认参与。DI 图的语义分支钉死。
func TestProvideRolesRWOnly(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	m := metrics.New(prometheus.NewRegistry())
	ex := ProvideExecutor(cli, reg, m, ProvideTelemetry(cli), ProvideBucketMap(cli, reg), ProvideL4(cli, reg), ProvideNearCache(), ProvideSyncClock(cli))
	require.NotNil(t, ex)

	store := ProvideCatalogStore(cli, reg)
	cache := ProvideCatalogCache(store, m)
	bm := ProvideBucketMap(cli, reg)

	roles := ProvideRoles(kidb.Bootstrap{}, cli, reg, store, cache, ex, bm, ProvideGuard(cli, reg, bm, ProvideSyncClock(cli)))
	require.NotNil(t, roles)
	require.NotNil(t, roles.Elector)
	require.NotNil(t, roles.Manager)
	require.NotNil(t, roles.JobRunner)
	require.NotNil(t, roles.Sweeper)
	require.NotNil(t, roles.Indexer)

	rwOnly := ProvideRoles(kidb.Bootstrap{ReadWriteOnly: true}, cli, reg, store, cache, ex, bm, ProvideGuard(cli, reg, bm, ProvideSyncClock(cli)))
	require.Nil(t, rwOnly)
}

// TestProvideEngineDepsGate 引擎依赖面必带全扫闸门（装配缺口即事故，钉死）。
func TestProvideEngineDepsGate(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	store := ProvideCatalogStore(cli, reg)
	m := metrics.New(prometheus.NewRegistry())
	deps := ProvideEngineDeps(cli, reg, store, ProvideCatalogCache(store, m),
		ProvideExecutor(cli, reg, m, ProvideTelemetry(cli), ProvideBucketMap(cli, reg), ProvideL4(cli, reg), ProvideNearCache(), ProvideSyncClock(cli)),
		ProvideGuard(cli, reg, ProvideBucketMap(cli, reg), ProvideSyncClock(cli)), m)
	require.NotNil(t, deps.FullscanGate)
}
