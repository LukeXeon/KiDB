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
// 过期堆的误删防线（Replace 在堆里留下新旧两条记录）：janitor 弹出时比对
// "堆记录的 deadline" 与 "ARC 当前条目的 deadline"——
//   - 相等 → 同一记录，删除；
//   - ARC 当前条目更晚 → 堆记录是 Replace 的陈旧残留，跳过（杜绝误删新值）。
package nearcache

import (
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

// Add 写入缓存（Replace 语义；堆中旧记录在 janitor 弹出比对时跳过）。
func (c *Cache[K, V]) Add(key K, val V) {
	dl := c.now().Add(c.ttl).UnixNano()
	c.arc.Add(key, entry[V]{val: val, deadline: dl})
	c.pq.Put(key, dl) // PQ 自带锁；重复 Put 的陈旧残留由弹出比对跳过
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

// evictExpired 摘除所有已到期条目（弹出比对纪律见包注释）。
// 堆按 deadline 有序：堆顶未到期 → 全部未到期，放回并结束本轮。
func (c *Cache[K, V]) evictExpired() {
	now := c.now().UnixNano()
	for {
		if c.pq.IsEmpty() {
			return
		}
		it := c.pq.Get()
		if it.Priority > now {
			c.pq.Put(it.Value, it.Priority) // 未到期：放回（弹出/放回净零）
			return
		}
		if cur, ok := c.arc.Peek(it.Value); ok && cur.deadline == it.Priority {
			c.arc.Remove(it.Value) // 同一记录才删；Replace 的更晚条目跳过
		}
	}
}

// Close 停 janitor（生命周期随内核 Close，docs/08 §8.4）。
func (c *Cache[K, V]) Close() error {
	close(c.stop)
	<-c.done
	return nil
}
