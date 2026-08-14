package nearcache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExpiryAndReadPathFallback(t *testing.T) {
	c, err := New[string, int](100, 30*time.Millisecond)
	require.NoError(t, err)
	defer c.Close()

	c.Add("a", 1)
	v, ok := c.Get("a")
	require.True(t, ok)
	require.Equal(t, 1, v)

	// 过期未触发 janitor 时读路径兜底（deadline 比较，docs/08 §8.4）
	c.now = func() time.Time { return time.Now().Add(40 * time.Millisecond) }
	_, ok = c.Get("a")
	require.False(t, ok, "过期值读路径必须拦截（不等 janitor）")
}

func TestReplaceNoMisdelete(t *testing.T) {
	c, err := New[string, int](100, time.Minute)
	require.NoError(t, err)
	defer c.Close()

	c.Add("k", 1)
	c.Add("k", 2) // Replace：旧堆元素不得误删新值
	v, ok := c.Get("k")
	require.True(t, ok)
	require.Equal(t, 2, v)
}

func TestJanitorReclaims(t *testing.T) {
	c, err := New[string, int](100, 30*time.Millisecond)
	require.NoError(t, err)
	defer c.Close()

	c.Add("hot", 1)
	require.Equal(t, 1, c.Len())
	require.Eventually(t, func() bool { return c.Len() == 0 }, 2*time.Second, 10*time.Millisecond,
		"janitor 应主动释放过期条目（冷条目不过夜）")
}

func TestJanitorExitsClean(t *testing.T) {
	c, err := New[string, int](10, time.Minute)
	require.NoError(t, err)
	c.Add("x", 1)
	require.NoError(t, c.Close()) // 退出无泄漏（goleak 防线在 CI，docs/12）
}
