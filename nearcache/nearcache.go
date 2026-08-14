// Package nearcache 实现 L1 进程内近缓存（docs/08 §8.4）：
// HashiCorp ARC（golang-lru/arc/v2，泛型）+ 自研 janitor 过期薄层
// （ARC 无内置 TTL：旁路最小堆 + 单一协程定时摘过期条目）。
//
// 职责分离（docs/08 §8.4 纪律）：
//   - 过期正确性由读路径兜底——value 携带 deadline，Get 时一次整数比较，
//     janitor 触发前的毫秒级窗口不会读出过期值；
//   - janitor 只负责主动释放内存（冷条目不过夜，不等 ARC 驱逐）。
//
// ARC v2.0.7 无驱逐回调：堆中残留"已被 ARC 驱逐"的条目在到期时 Remove 为 no-op，
// 不影响正确性（堆是活条目的超集）。
package nearcache

import (
	"container/heap"
	"sync"
	"time"

	arc "github.com/hashicorp/golang-lru/arc/v2"
)

// entry 是缓存值包装（deadline 在读路径校验）。
type entry[V any] struct {
	val      V
	deadline int64 // UnixNano
	heapIdx  int   // 堆内下标（janitor 维护）
}

// heapItem janitor 堆元素。
type heapItem[K comparable] struct {
	key      K
	deadline int64
}

type janitorHeap[K comparable] []heapItem[K]

func (h janitorHeap[K]) Len() int           { return len(h) }
func (h janitorHeap[K]) Less(i, j int) bool { return h[i].deadline < h[j].deadline }
func (h janitorHeap[K]) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *janitorHeap[K]) Push(x any)        { *h = append(*h, x.(heapItem[K])) }
func (h *janitorHeap[K]) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// Cache 是带 TTL 的 ARC 近缓存。
type Cache[K comparable, V any] struct {
	arc *arc.ARCCache[K, entry[V]]
	ttl time.Duration
	now func() time.Time

	mu   sync.Mutex
	heap janitorHeap[K]
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

// Add 写入缓存（Replace 语义：统一从堆侧先删旧值再插新值，
// 杜绝 janitor 误删新值——docs/08 §8.4）。
func (c *Cache[K, V]) Add(key K, val V) {
	dl := c.now().Add(c.ttl).UnixNano()
	c.mu.Lock()
	heap.Push(&c.heap, heapItem[K]{key: key, deadline: dl})
	c.mu.Unlock()
	c.arc.Add(key, entry[V]{val: val, deadline: dl})
}

// Remove 删除（堆侧条目到期时由 janitor 摘除为 no-op）。
func (c *Cache[K, V]) Remove(key K) { c.arc.Remove(key) }

// Len 当前容量。
func (c *Cache[K, V]) Len() int { return c.arc.Len() }

// janitor 单一协程：睡到堆顶 deadline，到期批量摘除。
func (c *Cache[K, V]) janitor() {
	defer close(c.done)
	var timer *time.Timer
	var timerC <-chan time.Time
	for {
		c.mu.Lock()
		if len(c.heap) > 0 {
			d := time.Until(time.Unix(0, c.heap[0].deadline))
			if timer == nil {
				timer = time.NewTimer(d)
				timerC = timer.C
			} else {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(d)
			}
		} else if timer != nil {
			timer.Stop()
			timerC = nil
			timer = nil
		}
		c.mu.Unlock()

		select {
		case <-c.stop:
			if timer != nil {
				timer.Stop()
			}
			return
		case <-timerC:
			c.evictExpired()
		}
	}
}

// evictExpired 摘除所有已到期条目（ARC 侧已驱逐的 Remove 为 no-op）。
func (c *Cache[K, V]) evictExpired() {
	now := c.now().UnixNano()
	for {
		c.mu.Lock()
		if len(c.heap) == 0 || c.heap[0].deadline > now {
			c.mu.Unlock()
			return
		}
		it := heap.Pop(&c.heap).(heapItem[K])
		c.mu.Unlock()
		c.arc.Remove(it.key)
	}
}

// Close 停 janitor（生命周期随内核 Close，docs/08 §8.4）。
func (c *Cache[K, V]) Close() error {
	close(c.stop)
	<-c.done
	return nil
}
