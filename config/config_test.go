package config

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"kidb/testutil"
)

func TestConfigStore(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	ctx := context.Background()
	s := New(cli, reg, "test")

	// 默认值（未显式设置）
	v, set, err := s.Get(ctx, "replica_read")
	require.NoError(t, err)
	require.Equal(t, "false", v)
	require.False(t, set)

	// SET + 读回
	require.NoError(t, s.Set(ctx, "replica_read", "true"))
	v, set, err = s.Get(ctx, "replica_read")
	require.NoError(t, err)
	require.Equal(t, "true", v)
	require.True(t, set)

	ver1, err := s.Version(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, ver1)

	// 校验拒绝：bool 变量只收 true/false
	require.Error(t, s.Set(ctx, "replica_read", "maybe"))
	// 表白名单格式校验
	require.Error(t, s.Set(ctx, "query_allow_fullscan_tables", "t1,,t2"))
	require.NoError(t, s.Set(ctx, "query_allow_fullscan_tables", "t1,t2"))
	// 未知变量拒绝（被杀的调优变量即未知变量——配置面收缩纪律）
	require.Error(t, s.Set(ctx, "no_such_var", "1"))
	require.Error(t, s.Set(ctx, "bucket_split_members", "60000"), "调优参数已转内置常量")

	// 变量表个位数纪律（docs/01 §1.0）
	require.Len(t, Vars, 3)

	ver2, _ := s.Version(ctx)
	require.Equal(t, ver1+1, ver2, "每次 SET 递增 _ver（传播锚点）")
}
