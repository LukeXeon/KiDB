package nearcache

import (
	"time"

	"github.com/maypok86/otter/v2"
)

// rowcache.go：行级近缓存（docs/08 §8.4 覆盖边界——pk→行投影，
// 缓释行级读热点 d:{table}:{pk}；默认关闭，hotkey_row_cache 变量开启）。
//
// 正确性纪律（与 L1 同族但更严）：
//   - 条目 TTL = min(默认 TTL, 行剩余 PTTL)——过期行绝不返回（物理一致）；
//   - 行内容更新有 ≤默认 TTL 的陈旧窗口（最终一致文档化语义——
//     正因此默认关闭，由部署方按业务容忍度开启）。

// rowEntry 缓存条目（ttl 随条目携带——otter ExpiryWritingFunc 按值计算）。
type rowEntry struct {
	Fields map[string]string
	TTL    time.Duration
}

// RowCache 是 pk→行字段的近缓存（otter 底座，逐条目 TTL）。
type RowCache struct {
	c          *otter.Cache[string, rowEntry]
	defaultTTL time.Duration // 无行 TTL 时的条目寿命上限
}

// NewRowCache 构造（capacity 条目上限；defaultTTL 为无 TTL 行的条目寿命）。
func NewRowCache(capacity int, defaultTTL time.Duration) *RowCache {
	c := otter.Must(&otter.Options[string, rowEntry]{
		MaximumSize: capacity,
		ExpiryCalculator: otter.ExpiryWritingFunc[string, rowEntry](func(e otter.Entry[string, rowEntry]) time.Duration {
			return e.Value.TTL
		}),
	})
	return &RowCache{c: c, defaultTTL: defaultTTL}
}

// Get 取缓存行（过期判定在 otter 读路径完成）。
func (c *RowCache) Get(key string) (map[string]string, bool) {
	e, ok := c.c.GetIfPresent(key)
	if !ok {
		return nil, false
	}
	return e.Fields, true
}

// Add 写入缓存：条目 TTL = min(defaultTTL, rowTTL)（rowTTL≤0 = 行无 TTL）。
func (c *RowCache) Add(key string, fields map[string]string, rowTTL time.Duration) {
	ttl := c.defaultTTL
	if rowTTL > 0 && rowTTL < ttl {
		ttl = rowTTL
	}
	c.c.Set(key, rowEntry{Fields: fields, TTL: ttl})
}

// Close 停止后台维护协程。
func (c *RowCache) Close() error { c.c.StopAllGoroutines(); return nil }
