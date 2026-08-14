// Package redistest 提供测试基建：miniredis + 参考适配器组装成
// 可用的 kidb.Client（docs/12 §12.2：单元/PBT 层跑在 miniredis 上）。
package redistest

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"kidb"
	"kidb/adapter/goredis"
	"kidb/script"
)

// New 起一台 miniredis，返回经参考适配器接线的 Client 与脚本注册表。
// ClusterSlots 覆盖把全部 slot 指到这台单实例——单实例无 CROSSSLOT 强制，
// 跨 slot 契约由真实集群的一致性测试套件覆盖（docs/12 §12.4）。
func New(t *testing.T) (kidb.Client, *script.Registry, *miniredis.Miniredis) {
	t.Helper()
	m := miniredis.RunT(t)
	addr := m.Addr()
	cli := goredis.New([]string{addr}, goredis.Options{
		ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
			return []redis.ClusterSlot{{
				Start: 0,
				End:   16383,
				Nodes: []redis.ClusterNode{{Addr: addr}},
			}}, nil
		},
	})
	t.Cleanup(func() { _ = cli.Close() })
	reg, err := script.Load()
	if err != nil {
		t.Fatalf("script.Load: %v", err)
	}
	return cli, reg, m
}
