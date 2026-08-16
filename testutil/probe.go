package testutil

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"kidb/kv"

	"github.com/stretchr/testify/require"
)

// probe.go：测试用 Redis 状态探针（sweeper/txguard 等 PBT 与不变式断言的
// 共用基建——此前各测试包各抄一份）。

// Probe 是绑定测试与客户端的断言探针。
type Probe struct {
	t   *testing.T
	cli kv.Client
}

// NewProbe 构造。
func NewProbe(t *testing.T, cli kv.Client) Probe {
	t.Helper()
	return Probe{t: t, cli: cli}
}

// ZScore ZSCORE（无成员返回 ""）。
func (p Probe) ZScore(key, member string) string {
	res, err := p.cli.Do(context.Background(), "ZSCORE", key, member)
	require.NoError(p.t, err)
	if res == nil {
		return ""
	}
	return fmt.Sprint(res)
}

// HGet HGET（无字段返回 ""）。
func (p Probe) HGet(key, field string) string {
	res, err := p.cli.Do(context.Background(), "HGET", key, field)
	require.NoError(p.t, err)
	if res == nil {
		return ""
	}
	return fmt.Sprint(res)
}

// Get GET（无 key 返回 ""）。
func (p Probe) Get(key string) string {
	res, err := p.cli.Do(context.Background(), "GET", key)
	require.NoError(p.t, err)
	if res == nil {
		return ""
	}
	return fmt.Sprint(res)
}

// Exists EXISTS。
func (p Probe) Exists(key string) bool {
	res, err := p.cli.Do(context.Background(), "EXISTS", key)
	require.NoError(p.t, err)
	return fmt.Sprint(res) == "1"
}

// PTTL PTTL（毫秒；-1=无 TTL，-2=不存在）。
func (p Probe) PTTL(key string) int64 {
	res, err := p.cli.Do(context.Background(), "PTTL", key)
	require.NoError(p.t, err)
	n, _ := strconv.ParseInt(fmt.Sprint(res), 10, 64)
	return n
}
