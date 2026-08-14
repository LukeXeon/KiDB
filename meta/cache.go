package meta

import (
	"context"
	"sync"
	"time"
)

// CatalogCache 是 Catalog 的本地缓存（docs/06 §6.2 lease 纪律）：
// 租约内信任本地快照；越界比对 ver:schema，版本变则整体失效重载。
// 正确性由写路径 Lua 内 CAS 兜底——本缓存只是性能优化。
type CatalogCache struct {
	store *CatalogStore
	lease *LeaseTracker

	mu     sync.RWMutex
	tables map[string]*TableDef
	clock  func() time.Time
}

// NewCatalogCache 构造（lease 默认 1s）。
func NewCatalogCache(store *CatalogStore) *CatalogCache {
	return &CatalogCache{
		store:  store,
		lease:  NewLeaseTracker(time.Second),
		tables: make(map[string]*TableDef),
		clock:  time.Now,
	}
}

// Get 取表定义；表不存在返回 (nil, nil)。
func (c *CatalogCache) Get(ctx context.Context, name string) (*TableDef, error) {
	if err := c.checkLease(ctx); err != nil {
		return nil, err
	}
	c.mu.RLock()
	def, ok := c.tables[name]
	c.mu.RUnlock()
	if ok {
		return def, nil
	}
	// 缓存未命中：回源并载入
	def, err := c.store.Load(ctx, name)
	if err != nil {
		return nil, err
	}
	if def == nil {
		return nil, nil
	}
	c.mu.Lock()
	c.tables[name] = def
	c.mu.Unlock()
	return def, nil
}

// Invalidate 作废旧快照（DDL 变更后调用）。
func (c *CatalogCache) Invalidate() {
	c.mu.Lock()
	c.tables = make(map[string]*TableDef)
	c.mu.Unlock()
	c.lease.Invalidate()
}

// checkLease 执行"越界必检"：租约外比对全局 schema 版本，变了清空缓存。
func (c *CatalogCache) checkLease(ctx context.Context) error {
	now := c.clock()
	if c.lease.Fresh(now) {
		return nil
	}
	remote, err := c.store.SchemaVersion(ctx)
	if err != nil {
		return err
	}
	if c.lease.Checked(now, remote) {
		c.mu.Lock()
		c.tables = make(map[string]*TableDef)
		c.mu.Unlock()
	}
	return nil
}
