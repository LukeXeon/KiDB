package nearcache

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// === ShardedCache 与 Cache 同一纪律的等价测试（docs/12 §12.2）===

func TestShardedExpiryReadPath(t *testing.T) {
	c := NewSharded[int](100, 30*time.Millisecond)
	defer c.Close()
	c.Add("a", 1)
	v, ok := c.Get("a")
	require.True(t, ok)
	require.Equal(t, 1, v)

	c.now = func() time.Time { return time.Now().Add(40 * time.Millisecond) }
	_, ok = c.Get("a")
	require.False(t, ok, "过期值读路径必须拦截")
}

func TestShardedReplaceAndSweep(t *testing.T) {
	c := NewSharded[int](100, time.Minute)
	defer c.Close()
	t0 := time.Now()
	now := t0
	c.now = func() time.Time { return now }

	c.Add("k", 1)
	now = t0.Add(10 * time.Second)
	c.Add("k", 2)
	now = t0.Add(61 * time.Second)

	c.sweepOnce()
	v, ok := c.Get("k")
	require.True(t, ok, "Replace 的新值不得被清扫误删（单结构无陈旧记录问题）")
	require.Equal(t, 2, v)

	now = t0.Add(71 * time.Second)
	c.sweepOnce()
	_, ok = c.Get("k")
	require.False(t, ok)
}

func TestShardedConcurrentStorm(t *testing.T) {
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
	require.Eventually(t, func() bool { return c.Len() == 0 }, 2*time.Second, 20*time.Millisecond)
}

func TestShardedCapEviction(t *testing.T) {
	c := NewSharded[int](128, time.Minute) // capPerShard = 1
	defer c.Close()
	for i := 0; i < 500; i++ {
		c.Add("k"+strconv.Itoa(i), i)
	}
	require.LessOrEqual(t, c.Len(), 128, "容量上限必须生效")
}

// sweepOnce 供测试手动驱动一轮清扫（与 sweeper 同构）。
func (c *ShardedCache[V]) sweepOnce() {
	deadline := c.now().UnixNano()
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		for k, e := range sh.m {
			if e.deadline <= deadline {
				delete(sh.m, k)
			}
		}
		sh.mu.Unlock()
	}
}

// === 基准：v1（ARC+PQ+janitor）vs v2（分片 map）===

var benchKeys []string

func init() {
	for i := 0; i < 256; i++ {
		benchKeys = append(benchKeys, "fingerprint|idx|v"+strconv.Itoa(i))
	}
}

func BenchmarkV2Mixed(b *testing.B) {
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
			k := benchKeys[i&(len(benchKeys)-1)]
			if i%8 == 0 {
				c.Add(k, pks)
			} else {
				_, _ = c.Get(k)
			}
			i++
		}
	})
}
