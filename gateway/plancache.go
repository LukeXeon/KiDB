package gateway

import (
	"container/list"
	"sync"
)

// plancache.go：KiDB 侧计划缓存（docs/02 §2.6）。
//
// 缓存的不是执行计划结构（gms 有自己的语句缓存），而是**网关层的判定**：
// 指纹（parser.NormalizeDigest，lexer 级归一不出 AST）→
// {快速路径形状（含表/列）、守卫放行判定}——同指纹必然同列名集合，
// 守卫判定在指纹内稳定（索引存在性/白名单外的判定不随字面量变化）。
//
// 版本绑定纪律（对齐 TiDB plan cache）：条目记录生成时的全局 schema 版本；
// 命中前比对当前快照版本（lease 内零 RTT），不一致即弃用重建——
// DDL 上线/索引可见后旧判定绝不复用（惰性精确失效，无需广播）。
//
// 有意不缓存的：全扫依赖判定（hint/白名单放行或 ERR_NO_INDEX 拒绝）——
// 它们依赖 query_allow_fullscan_tables 配置（变更不走 schema 版本），
// 保守不缓存，每次现算。

// planDecision 一次缓存的判定。
type planDecision struct {
	schemaVer uint64    // 生成时的全局 schema 版本
	fp        *fastPath // 快速路径形状（nil = 未命中）
}

// planCache LRU（容量 plan_cache_capacity，默认 1024）。
type planCache struct {
	mu    sync.Mutex
	cap   int
	ll    *list.List               // 前置 = 最近用
	items map[string]*list.Element // digest → 元素
}

type planEntry struct {
	digest string
	pd     planDecision
}

func newPlanCache(capacity int) *planCache {
	if capacity <= 0 {
		capacity = 1024
	}
	return &planCache{cap: capacity, ll: list.New(), items: map[string]*list.Element{}}
}

// get 命中返回判定；schemaVer 不匹配即弃用（stale）。
// stale=true 且 hit=false = 版本失效逐出（plan_cache_stale_total 挂点）。
func (c *planCache) get(digest string, schemaVer uint64) (planDecision, bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[digest]
	if !ok {
		return planDecision{}, false, false
	}
	pd := el.Value.(*planEntry).pd
	if pd.schemaVer != schemaVer {
		c.ll.Remove(el)
		delete(c.items, digest)
		return planDecision{}, false, true
	}
	c.ll.MoveToFront(el)
	return pd, true, false
}

// put 写入/更新（LRU 逐出最旧）。
func (c *planCache) put(digest string, pd planDecision) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[digest]; ok {
		el.Value.(*planEntry).pd = pd
		c.ll.MoveToFront(el)
		return
	}
	el := c.ll.PushFront(&planEntry{digest, pd})
	c.items[digest] = el
	for c.ll.Len() > c.cap {
		tail := c.ll.Back()
		c.ll.Remove(tail)
		delete(c.items, tail.Value.(*planEntry).digest)
	}
}

// resize 容量热更新（plan_cache_capacity 变量）。
func (c *planCache) resize(capacity int) {
	if capacity <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cap = capacity
	for c.ll.Len() > c.cap {
		tail := c.ll.Back()
		c.ll.Remove(tail)
		delete(c.items, tail.Value.(*planEntry).digest)
	}
}
