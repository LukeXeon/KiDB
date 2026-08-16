// Package testutil 提供测试基建：miniredis + 参考适配器组装成
// 可用的 kidb.KvClient（docs/12 §12.2：单元/PBT 层跑在 miniredis 上）。
package testutil

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"kidb"
	"kidb/adapter/goredis"
	"kidb/script"
)

// New 起一台 miniredis，返回经参考适配器接线的 Client 与脚本注册表。
// ClusterSlots 覆盖把全部 slot 指到这台单实例——单实例无 CROSSSLOT 强制，
// 跨 slot 契约由真实集群的一致性测试套件覆盖（docs/12 §12.4）。
func New(t testing.TB) (kidb.KvClient, *script.Registry, *miniredis.Miniredis) {
	t.Helper()
	m := miniredis.RunT(t)
	addr := m.Addr()
	// 与生产同形：适配器外包退避矩阵装饰器（di.ProvideClient 同款）
	raw := goredis.New([]string{addr}, goredis.Options{
		ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
			return []redis.ClusterSlot{{
				Start: 0,
				End:   16383,
				Nodes: []redis.ClusterNode{{Addr: addr}},
			}}, nil
		},
	})
	cli := kidb.NewRetryingClient(raw, kidb.DefaultRetryPolicy())
	t.Cleanup(func() { _ = raw.Close() })
	reg, err := script.Load()
	if err != nil {
		t.Fatalf("script.Load: %v", err)
	}
	return cli, reg, m
}

// ServerClock 返回跟随 miniredis 服务端时间的时钟（FastForward 同步生效）。
// 写入/清扫两侧在 TTL 测试中必须共用同一时钟——miniredis 的 FastForward 只移动
// 服务端时间，客户端本地时钟不走（生产侧由 TIME 校准，docs/11 §11.1 时钟偏移行）。
func ServerClock(cli kidb.KvClient) func() time.Time {
	return func() time.Time {
		res, err := cli.Do(context.Background(), "TIME")
		if err != nil {
			return time.Now()
		}
		ss, ok := res.([]any)
		if !ok || len(ss) == 0 {
			return time.Now()
		}
		n, err := strconv.ParseInt(fmt.Sprint(ss[0]), 10, 64)
		if err != nil {
			return time.Now()
		}
		return time.Unix(n, 0)
	}
}
