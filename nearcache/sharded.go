// Package nearcache — L1 进程内近缓存。
//
// sharded.go 是 v2 推荐实现（docs/08 §8.4）：分片 map + 值内 deadline + 周期清扫。
//
// 与 v1（ARC+PQ+janitor）的差异与动机：
//   - HashiCorp ARC 全结构一把互斥锁（Get 也抢锁），是 L1 热路径的并发瓶颈；
//     ARC 的自适应抗扫描价值被"全扫指纹不入缓存"规则对冲，边际≈0；
//   - 本实现唯一结构就是分片 map：清扫删除与 Add 同在分片写锁内——
//     v1 的"弹出比对 + 互斥临界区"纪律整个不再需要（结构上无 TOCTOU）；
//   - 读路径 deadline 校验兜底过期的纪律不变（docs/08 §8.4）。
//
// 淘汰策略：分片容量超限时随机驱逐（TTL 3s 场景过期压力主导，驱逐极少触发）；
// 扫描对抗的补强（二次触摸准入）列为观察项，当前不引入。
package nearcache

import (
	"hash/fnv"
	"sync"
	"time"
)

// entry 是缓存值包装（deadline 供读路径校验 + 清扫删除判定）。
type entry[V any] struct {
	val      V
	deadline int64 // UnixNano
}

// shardCount 分片数（2 的幂，取模即掩码）。
const shardCount = 128

type shard[V any] struct {
	mu sync.RWMutex
	m  map[string]entry[V]
}

// ShardedCache 是 v2 L1 缓存（string key 特化——谓词指纹）。
type ShardedCache[V any] struct {
	shards      [shardCount]shard[V]
	ttl         time.Duration
	capPerShard int
	now         func() time.Time
	stop        chan struct{}
	done        chan struct{}
}

// NewSharded 构造（capacity 总上限按分片均摊；启动清扫协程）。
func NewSharded[V any](capacity int, ttl time.Duration) *ShardedCache[V] {
	c := &ShardedCache[V]{
		ttl:         ttl,
		capPerShard: max(1, capacity/shardCount),
		now:         time.Now,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	for i := range c.shards {
		c.shards[i].m = make(map[string]entry[V])
	}
	go c.sweeper()
	return c
}

func (c *ShardedCache[V]) shardOf(key string) *shard[V] {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &c.shards[h.Sum32()&(shardCount-1)]
}

// Get 取缓存：读锁 + deadline 校验（过期即未命中，不等清扫）。
func (c *ShardedCache[V]) Get(key string) (V, bool) {
	var zero V
	sh := c.shardOf(key)
	sh.mu.RLock()
	e, ok := sh.m[key]
	sh.mu.RUnlock()
	if !ok {
		return zero, false
	}
	if c.now().UnixNano() > e.deadline {
		return zero, false
	}
	return e.val, true
}

// Add 写入（写锁内完成；超限随机驱逐——Go map range 首元素即伪随机）。
func (c *ShardedCache[V]) Add(key string, val V) {
	sh := c.shardOf(key)
	dl := c.now().Add(c.ttl).UnixNano()
	sh.mu.Lock()
	sh.m[key] = entry[V]{val: val, deadline: dl}
	for len(sh.m) > c.capPerShard {
		for k := range sh.m {
			delete(sh.m, k) // 随机驱逐一个
			break
		}
	}
	sh.mu.Unlock()
}

// Remove 删除。
func (c *ShardedCache[V]) Remove(key string) {
	sh := c.shardOf(key)
	sh.mu.Lock()
	delete(sh.m, key)
	sh.mu.Unlock()
}

// Len 当前总条数（诊断用，逐分片加总）。
func (c *ShardedCache[V]) Len() int {
	n := 0
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.RLock()
		n += len(sh.m)
		sh.mu.RUnlock()
	}
	return n
}

// sweeper 周期清扫：逐分片持写锁删过期项——与 Add 同锁，无 TOCTOU。
// 间隔 min(ttl/4, 100ms)；到期正确性由读路径兜底，此处只争释放时机。
func (c *ShardedCache[V]) sweeper() {
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
		case now := <-t.C:
			deadline := now.UnixNano()
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
	}
}

// Close 停清扫协程（生命周期随内核 Close）。
func (c *ShardedCache[V]) Close() error {
	close(c.stop)
	<-c.done
	return nil
}
