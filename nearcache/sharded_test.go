package nearcache

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// === L1 纪律测试（docs/12 §12.2；v3 底座 otter，读路径过期判定实证于 otter getNode）===

func TestExpiryReadPath(t *testing.T) {
	c := NewSharded[int](100, 30*time.Millisecond)
	defer c.Close()
	c.Add("a", 1)
	v, ok := c.Get("a")
	require.True(t, ok)
	require.Equal(t, 1, v)

	require.Eventually(t, func() bool {
		_, ok := c.Get("a")
		return !ok
	}, 2*time.Second, 10*time.Millisecond, "过期值读路径必须拦截（docs/08 §8.4 第一纪律）")
}

func TestReplace(t *testing.T) {
	c := NewSharded[int](100, time.Minute)
	defer c.Close()
	c.Add("k", 1)
	c.Add("k", 2)
	v, ok := c.Get("k")
	require.True(t, ok)
	require.Equal(t, 2, v, "Replace 返回最新值")
}

func TestConcurrentStorm(t *testing.T) {
	c := NewSharded[int](1000, 30*time.Millisecond)
	defer c.Close()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
					k := "key-" + strconv.Itoa((id+i)%64)
					c.Add(k, i)
					_, _ = c.Get(k)
				}
			}
		}(w)
	}
	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
	// 过期后所有 key 不可读（物理清理由 otter 后台进行，语义以读为准）
	require.Eventually(t, func() bool {
		for i := 0; i < 64; i++ {
			if _, ok := c.Get("key-" + strconv.Itoa(i)); ok {
				return false
			}
		}
		return true
	}, 2*time.Second, 20*time.Millisecond)
}

func TestCapEviction(t *testing.T) {
	c := NewSharded[int](128, time.Minute)
	defer c.Close()
	for i := 0; i < 500; i++ {
		c.Add("k"+strconv.Itoa(i), i)
	}
	require.Eventually(t, func() bool { return c.Len() <= 160 }, 2*time.Second, 10*time.Millisecond,
		"容量上限必须生效（W-TinyLFU 驱逐，允许小偏差）")
}

// === 基准 ===

var benchKeys []string

func init() {
	for i := 0; i < 256; i++ {
		benchKeys = append(benchKeys, "fingerprint|idx|v"+strconv.Itoa(i))
	}
}

func BenchmarkMixed(b *testing.B) {
	c := NewSharded[[]string](10000, 3*time.Second)
	defer c.Close()
	pks := []string{"1", "2", "3"}
	for _, k := range benchKeys {
		c.Add(k, pks)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := benchKeys[i%len(benchKeys)]
			if i%4 == 0 {
				c.Add(k, pks)
			} else {
				_, _ = c.Get(k)
			}
			i++
		}
	})
}
