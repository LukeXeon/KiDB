package config

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"kidb/internal/redistest"
)

func TestConfigStore(t *testing.T) {
	cli, reg, _ := redistest.New(t)
	ctx := context.Background()
	s := New(cli, reg, "test")

	// 默认值（未显式设置）
	v, set, err := s.Get(ctx, "bucket_split_members")
	require.NoError(t, err)
	require.Equal(t, "50000", v)
	require.False(t, set)

	// SET + 读回
	require.NoError(t, s.Set(ctx, "bucket_split_members", "60000"))
	v, set, err = s.Get(ctx, "bucket_split_members")
	require.NoError(t, err)
	require.Equal(t, "60000", v)
	require.True(t, set)

	ver1, err := s.Version(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, ver1)

	// 校验拒绝：超 16MB 红线（docs/10 §10.2 校验规则）
	require.Error(t, s.Set(ctx, "bucket_split_bytes", "33554432"))
	// 未知变量拒绝
	require.Error(t, s.Set(ctx, "no_such_var", "1"))
	// 枚举校验
	require.Error(t, s.Set(ctx, "hotkey_source", "bogus"))
	require.NoError(t, s.Set(ctx, "hotkey_source", "both"))

	// All（SHOW GLOBAL VARIABLES 数据源）
	all, err := s.All(ctx)
	require.NoError(t, err)
	require.Equal(t, "60000", all["bucket_split_members"])
	require.Equal(t, "both", all["hotkey_source"])
	require.NotContains(t, all, "_ver", "内部字段不外露")

	ver2, _ := s.Version(ctx)
	require.Equal(t, ver1+1, ver2, "每次 SET 递增 _ver（传播锚点）")
}
