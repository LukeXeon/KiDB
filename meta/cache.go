package meta

import (
	"context"

	"kidb/metrics"
	"kidb/tuning"
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
	m      *metrics.Metrics // nil = no-op（schema_lease_refresh_total）
}

// SetMetrics 接入指标（版本变化触发的缓存重建计数）。
func (c *CatalogCache) SetMetrics(m *metrics.Metrics) { c.m = m }

// NewCatalogCache 构造（lease 默认 1s）。
func NewCatalogCache(store *CatalogStore) *CatalogCache {
	return &CatalogCache{
		store:  store,
		lease:  NewLeaseTracker(tuning.Get().SchemaLease()),
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

// SchemaVersion 返回当前快照的全局 schema 版本（plan cache 版本绑定的锚，
// docs/02 §2.6）：租约内零 RTT（本地快照版本），越界先执行 lease 校验。
func (c *CatalogCache) SchemaVersion(ctx context.Context) (uint64, error) {
	if err := c.checkLease(ctx); err != nil {
		return 0, err
	}
	return c.lease.Version(), nil
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
		if c.m != nil {
			c.m.LeaseRefresh.Inc()
		}
	}
	return nil
}
