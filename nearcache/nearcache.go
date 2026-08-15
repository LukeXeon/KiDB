// Package nearcache 实现 L1 进程内近缓存（docs/08 §8.4）：
// HashiCorp ARC（golang-lru/arc/v2，泛型）+ janitor 过期薄层
// （过期堆用 gopkg.in/dnaeon/go-priorityqueue.v1——选型表既有组件，
// 与后续 top-k 归并共用同一依赖）。
//
// 职责分离（docs/08 §8.4 纪律）：
//   - 过期正确性由读路径兜底——value 携带 deadline，Get 时一次整数比较，
//     janitor 触发前的毫秒级窗口不会读出过期值；
//   - janitor 只负责主动释放内存（冷条目不过夜，不等 ARC 驱逐）。
//
// 过期堆的防误删纪律（Replace 在堆里留下新旧两条记录）：
//   - janitor 弹出时比对"堆记录 deadline"与"ARC 当前条目 deadline"——相等才删，
//     当前条目更晚则跳过（陈旧记录）；
//   - **比对与删除必须在同一临界区内**（Add 与摘除经 mu 互斥）——否则
//     Peek→Remove 之间存在 TOCTOU 窗口：Replace 恰好落在窗口内会误删新值。
//     Get/Remove 不进临界区（读与删不创造新条目，ARC 自带锁）。
package nearcache

import (
	"sync"
	"time"

	arc "github.com/hashicorp/golang-lru/arc/v2"
	pq "gopkg.in/dnaeon/go-priorityqueue.v1"
)

// entry 是缓存值包装（deadline 供读路径校验 + janitor 弹出比对）。
type entry[V any] struct {
	val      V
	deadline int64 // UnixNano
}

// Cache 是带 TTL 的 ARC 近缓存。
type Cache[K comparable, V any] struct {
	arc *arc.ARCCache[K, entry[V]]
	ttl time.Duration
	now func() time.Time

	mu   sync.Mutex                  // Add 与 janitor 摘除的互斥（防误删临界区）
	pq   *pq.PriorityQueue[K, int64] // janitor 过期堆（最小堆，按 deadline）
	stop chan struct{}
	done chan struct{}
}

// New 构造（capacity 上限，ttl 过期时间；启动 janitor 协程）。
func New[K comparable, V any](capacity int, ttl time.Duration) (*Cache[K, V], error) {
	a, err := arc.NewARC[K, entry[V]](capacity)
	if err != nil {
		return nil, err
	}
	c := &Cache[K, V]{
		arc:  a,
		ttl:  ttl,
		now:  time.Now,
		pq:   pq.New[K, int64](pq.MinHeap),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go c.janitor()
	return c, nil
}

// Get 取缓存：读路径兜底过期（deadline 整数比较，docs/08 §8.4）。
// 不进互斥临界区——热路径零影响。
func (c *Cache[K, V]) Get(key K) (V, bool) {
	var zero V
	e, ok := c.arc.Get(key)
	if !ok {
		return zero, false
	}
	if c.now().UnixNano() > e.deadline {
		return zero, false // 过期即未命中（janitor 稍后物理摘除）
	}
	return e.val, true
}

// Add 写入缓存（Replace 语义；与 janitor 摘除互斥，见包注释防误删纪律）。
func (c *Cache[K, V]) Add(key K, val V) {
	dl := c.now().Add(c.ttl).UnixNano()
	c.mu.Lock()
	c.arc.Add(key, entry[V]{val: val, deadline: dl})
	c.pq.Put(key, dl) // 重复 Put 的陈旧残留由弹出比对跳过
	c.mu.Unlock()
}

// Remove 删除（堆中记录到期弹出时比对跳过，为无害残留）。
func (c *Cache[K, V]) Remove(key K) { c.arc.Remove(key) }

// Len 当前容量。
func (c *Cache[K, V]) Len() int { return c.arc.Len() }

// janitor 周期清扫：tick 间隔为 TTL 的 1/4（上限 100ms）——
// 到期正确性由读路径兜底，此处只争释放时机，周期清扫足够。
func (c *Cache[K, V]) janitor() {
	defer close(c.done)
	interval := c.ttl / 4
	if interval <= 0 || interval > 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			c.evictExpired()
		}
	}
}

// evictExpired 摘除所有已到期条目。每条目一个临界区：
// 弹出 →（堆顶未到期则放回并结束本轮）→ 比对 → 删除，全程持 mu；
// Add 因此不可能插在"比对→删除"之间（TOCTOU 窗口关闭）。
// 堆按 deadline 有序：堆顶未到期 → 全部未到期。
func (c *Cache[K, V]) evictExpired() {
	now := c.now().UnixNano()
	for {
		c.mu.Lock()
		if c.pq.IsEmpty() {
			c.mu.Unlock()
			return
		}
		it := c.pq.Get()
		if it.Priority > now {
			c.pq.Put(it.Value, it.Priority) // 未到期：放回（弹出/放回净零）
			c.mu.Unlock()
			return
		}
		if cur, ok := c.arc.Peek(it.Value); ok && cur.deadline == it.Priority {
			c.arc.Remove(it.Value) // 同一记录才删；Replace 的更晚条目跳过
		}
		c.mu.Unlock()
	}
}

// Close 停 janitor（生命周期随内核 Close，docs/08 §8.4）。
func (c *Cache[K, V]) Close() error {
	close(c.stop)
	<-c.done
	return nil
}
