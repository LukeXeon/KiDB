// Package nearcache — L1 进程内近缓存（docs/08 §8.4）。
//
// v3：底座换 maypok86/otter（v2）。选型裁决（替换自研分片 map）：
//   - 第一纪律"过期正确性由读路径兜底"——otter getNode 在读路径做
//     HasExpired(nowNano) 判定，过期值绝不返回（cache_impl.go 实证）；
//   - 内部分片并发 + W-TinyLFU 抗扫描（对全扫指纹更友好）+ 内置命中率统计；
//   - 删掉自研组件 = 维护面 -1（能用库的用库，docs/10 §10.1 纪律）。
package nearcache

import (
	"time"

	"github.com/maypok86/otter/v2"
)

// ShardedCache 是 L1 缓存（string key 特化——谓词指纹）。
// 类型名保留以兼容既有调用点；底座为 otter。
type ShardedCache[V any] struct {
	c *otter.Cache[string, V]
}

// NewSharded 构造（capacity 条目上限；ttl 过期时间）。
func NewSharded[V any](capacity int, ttl time.Duration) *ShardedCache[V] {
	c := otter.Must(&otter.Options[string, V]{
		MaximumSize:      capacity,
		ExpiryCalculator: otter.ExpiryWriting[string, V](ttl),
	})
	return &ShardedCache[V]{c: c}
}

// Get 取缓存（读路径过期判定在 otter 内完成，超 TTL 不返回）。
func (c *ShardedCache[V]) Get(key string) (V, bool) {
	return c.c.GetIfPresent(key)
}

// Add 写入缓存。
func (c *ShardedCache[V]) Add(key string, val V) { c.c.Set(key, val) }

// Remove 删除。
func (c *ShardedCache[V]) Remove(key string) { c.c.Invalidate(key) }

// Len 估算容量（otter 口径，含待清理过期项）。
func (c *ShardedCache[V]) Len() int { return c.c.EstimatedSize() }

// Close 关闭（停止 otter 后台维护协程）。
func (c *ShardedCache[V]) Close() error { c.c.StopAllGoroutines(); return nil }
