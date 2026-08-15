package nearcache

import (
	"strconv"
	"sync"
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

// TestStaleHeapRecordNeverMisdeletes 回归测试（真实缺陷钉死）：
// Replace 在过期堆里留下新旧两条记录；旧记录到期弹出时不得删除新值。
// 用假钟手动驱动 evictExpired，确定性无睡眠。
func TestStaleHeapRecordNeverMisdeletes(t *testing.T) {
	c, err := New[string, int](100, time.Minute) // ttl 1min，假钟驱动
	require.NoError(t, err)
	defer c.Close()

	t0 := time.Now()
	now := t0
	c.now = func() time.Time { return now }

	c.Add("k", 1) // 堆记录 dl = t0+60s
	now = t0.Add(10 * time.Second)
	c.Add("k", 2)                  // Replace：堆里新增 dl = t0+70s，ARC 当前条目 = v2
	now = t0.Add(65 * time.Second) // 越过旧记录 deadline（60s），未到新记录（70s）

	c.evictExpired()
	v, ok := c.Get("k")
	require.True(t, ok, "新值不得在旧堆记录到期时被误删")
	require.Equal(t, 2, v)

	now = t0.Add(71 * time.Second) // 越过新记录 deadline
	c.evictExpired()
	_, ok = c.Get("k")
	require.False(t, ok, "真到期后 janitor 应摘除")
}

func TestJanitorExitsClean(t *testing.T) {
	c, err := New[string, int](10, time.Minute)
	require.NoError(t, err)
	c.Add("x", 1)
	require.NoError(t, c.Close()) // 退出无泄漏
}

// TestConcurrentAddVsJanitor 并发 Add/Get 与 janitor 清扫对撞（-race 运行）：
// 不停 Replace 同批 key + 短 TTL，结束后越过全部 deadline → 缓存必须排空。
// 防误删纪律经互斥临界区保证（包注释）；本测试钉住"无数据竞争 + 最终排空"。
func TestConcurrentAddVsJanitor(t *testing.T) {
	c, err := New[string, int](1000, 30*time.Millisecond)
	require.NoError(t, err)
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
	time.Sleep(150 * time.Millisecond) // 多轮 Replace + 多轮清扫
	close(stop)
	wg.Wait()

	// 静止后越过全部 deadline：janitor 必须排空
	require.Eventually(t, func() bool { return c.Len() == 0 },
		2*time.Second, 20*time.Millisecond, "全部到期后必须排空（含陈旧堆记录场景）")
}
