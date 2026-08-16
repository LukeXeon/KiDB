// Package controller 的 L4 热桶副本管理（docs/08 §8.4）：
// 超阈自动建 K 个异 slot 只读副本（stag 步进替换），1s 滚动刷新，热度回落自动回收。
// 最终一致窗口（≤刷新周期）：副本内行级新增/变更在窗口内不可见——
// 与异步索引/副本读同级的文档化语义（docs/14 §14.2 最终一致窗口）。
//
// 粒度纪律（review 实证）：注册表字段按 **桶**（值 × slot）——`r4:{encVal}:{slot}`。
// 按值注册会把所有 slot 的读重定向到单一 slot 的副本上 = 其他 slot 的行凭空消失。
// 生命周期纪律：副本 60s TTL 只是兜底——活跃度由本管理器的周期 Tick 驱动
// （刷新续期 / 连续冷却回收），读路径对死副本经同 pipeline EXISTS 判定回退源桶。
package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"kidb/keycodec"
	"kidb/kv"
	"kidb/meta"
	"kidb/metrics"
	"kidb/script"
	"kidb/telemetry"
	"kidb/utils"
)

// L4Manager 管理热桶副本。
type L4Manager struct {
	cli    kv.Client
	reg    *script.Registry
	maxRep int              // hotkey_replica_max（默认 8）
	ttlMs  int64            // 副本 TTL（60s 滚动兜底）
	m      *metrics.Metrics // nil = no-op（hotkey_replicas_active）

	cold map[string]int // 字段 → 连续冷却 tick 数（回收计数）

	repMu    sync.RWMutex
	repCache map[string]repEnt // ReplicaFor 注册读缓存（1s——热路径每查询一次 HGET 不可接受）
}

type repEnt struct {
	k  int
	at time.Time
}

// SetMetrics 接入指标。
func (m *L4Manager) SetMetrics(mt *metrics.Metrics) { m.m = mt }

// NewL4 构造。
func NewL4(cli kv.Client, reg *script.Registry) *L4Manager {
	return &L4Manager{cli: cli, reg: reg, maxRep: 8, ttlMs: 60000, cold: map[string]int{}, repCache: map[string]repEnt{}}
}

// l4Field 注册表字段名（bmh:{table}:{idx} 内）：`r4:{源桶key}`——按桶粒度
// （v7.0：桶按值/索引寻址，源桶 key 即唯一身份；按值注册会把其他桶的读
// 重定向到单桶副本上——行凭空消失，v6.x review 实证形态）。
func l4Field(srcBucketKey string) string {
	return "r4:" + srcBucketKey
}

// parseL4Field 反解注册表字段 → 源桶 key。
func parseL4Field(f string) (srcBucketKey string, ok bool) {
	v, found := strings.CutPrefix(f, "r4:")
	if !found || v == "" {
		return "", false
	}
	return v, true
}

// Activate 为热桶建 K 个副本（K=⌈热QPS/单节点安全QPS⌉ ≤ maxRep）。
// srcBucketKey 为源桶 key；副本 key 由 keycodec.ReplicaKey 步进替换生成。
func (m *L4Manager) Activate(ctx context.Context, table, idxID, srcBucketKey string, k int) error {
	if k <= 0 {
		return nil
	}
	if k > m.maxRep {
		k = m.maxRep
	}
	if err := m.Refresh(ctx, srcBucketKey, k); err != nil {
		return err
	}
	// 注册：读路径据此切换到副本（bmh 注册表字段 r4:{源桶key} = K）
	f := l4Field(srcBucketKey)
	res, err := m.cli.Do(ctx, "HGET", keycodec.BucketMapHotKey(table, idxID), f)
	if err != nil {
		return err
	}
	if res != nil {
		return nil // 已激活
	}
	if _, err := m.cli.Do(ctx, "HSET", keycodec.BucketMapHotKey(table, idxID), f, strconv.Itoa(k)); err != nil {
		return err
	}
	if m.m != nil {
		m.m.HotReplicas.Add(float64(k))
	}
	return nil
}

// Refresh 全量读源桶 + 逐副本原子重建（replica_refresh.lua）。
func (m *L4Manager) Refresh(ctx context.Context, srcBucketKey string, k int) error {
	rf, ok := m.reg.Get("replica_refresh")
	if !ok {
		return fmt.Errorf("controller: replica_refresh.lua not registered")
	}
	res, err := m.cli.Do(ctx, "ZRANGE", srcBucketKey, 0, -1, "WITHSCORES")
	if err != nil {
		return err
	}
	arr := utils.AnySlice(res)
	for ri := 1; ri <= k; ri++ {
		argv := []any{strconv.FormatInt(m.ttlMs, 10), strconv.Itoa(len(arr) / 2)}
		for i := 0; i+1 < len(arr); i += 2 {
			argv = append(argv, fmt.Sprint(arr[i+1]), fmt.Sprint(arr[i])) // score, member
		}
		if _, err := m.cli.Eval(ctx, rf, []string{keycodec.ReplicaKey(srcBucketKey, ri)}, argv...); err != nil {
			return err
		}
	}
	return nil
}

// Tick L4 生命周期巡检（Controller 选举锁持有者经 Manager.Tick 驱动，1s 节奏）：
// 刷新全部活跃副本（续期）+ 连续冷却 3 tick 的回收（st:{桶} 60s 无采样命中即冷）。
func (m *L4Manager) Tick(ctx context.Context, store *meta.CatalogStore) error {
	tables, err := store.ListTables(ctx)
	if err != nil {
		return err
	}
	alive := utils.NewSet[string]()
	for _, name := range tables {
		def, err := store.Load(ctx, name)
		if err != nil || def == nil {
			continue
		}
		for i := range def.Indexes {
			idx := &def.Indexes[i]
			reg, err := m.registryAll(ctx, def.Name, idx.ID)
			if err != nil {
				continue
			}
			for f, ks := range reg {
				src, ok := parseL4Field(f)
				if !ok {
					continue
				}
				k, _ := strconv.Atoi(ks)
				if k <= 0 {
					continue
				}
				alive.Add(def.Name + "/" + idx.ID + "/" + f)
				// 冷度判定：st:{桶} 采样计数器缺失/无增量 = 冷
				ops := m.sampledOps(ctx, src)
				if ops == 0 {
					m.cold[f]++
					if m.cold[f] >= 3 {
						if err := m.Deactivate(ctx, def.Name, idx.ID, src); err == nil {
							delete(m.cold, f)
						}
						continue
					}
				} else {
					m.cold[f] = 0
				}
				_ = m.Refresh(ctx, src, k) // 失败下轮再来（副本 TTL 兜底 60s 窗口）
			}
		}
	}
	// 表/索引已删的残留注册项清计数（Deactivate 由 DROP 清理面负责 key 侧）
	for f := range m.cold {
		if !aliveAny(alive, f) {
			delete(m.cold, f)
		}
	}
	return nil
}

// aliveAny 粗匹配（计数器 key 含表/索引前缀）。
func aliveAny(alive utils.Set[string], f string) bool {
	for k := range alive {
		if strings.HasSuffix(k, "/"+f) {
			return true
		}
	}
	return false
}

// registryAll 读 bmh 全部字段（r4: 前缀过滤在调用方）。
func (m *L4Manager) registryAll(ctx context.Context, table, idxID string) (map[string]string, error) {
	res, err := m.cli.Do(ctx, "HGETALL", keycodec.BucketMapHotKey(table, idxID))
	if err != nil {
		return nil, err
	}
	return utils.StringMap(res)
}

// sampledOps 读桶采样计数（st:{桶} ops 字段；缺失 = 0）。
func (m *L4Manager) sampledOps(ctx context.Context, srcBucketKey string) int64 {
	res, err := m.cli.Do(ctx, "HGET", telemetry.StatKey(srcBucketKey), "ops")
	if err != nil || res == nil {
		return 0
	}
	n, _ := strconv.ParseInt(fmt.Sprint(res), 10, 64)
	return n
}

// Deactivate 回收副本（热度回落）：注销注册表 + UNLINK 全部副本。
func (m *L4Manager) Deactivate(ctx context.Context, table, idxID, srcBucketKey string) error {
	res, err := m.cli.Do(ctx, "HGET", keycodec.BucketMapHotKey(table, idxID), l4Field(srcBucketKey))
	if err != nil {
		return err
	}
	if res == nil {
		return nil
	}
	k, _ := strconv.Atoi(fmt.Sprint(res))
	for ri := 1; ri <= k; ri++ {
		if _, err := m.cli.Do(ctx, "UNLINK", keycodec.ReplicaKey(srcBucketKey, ri)); err != nil {
			return err
		}
	}
	if _, err := m.cli.Do(ctx, "HDEL", keycodec.BucketMapHotKey(table, idxID), l4Field(srcBucketKey)); err != nil {
		return err
	}
	m.repMu.Lock()
	delete(m.repCache, table+"/"+idxID+"/"+l4Field(srcBucketKey)) // 摘缓存同步摘除（Deactivate 语义即时生效）
	m.repMu.Unlock()
	if m.m != nil {
		m.m.HotReplicas.Add(-float64(k))
	}
	return nil
}

// ReplicaFor 读路径入口：若**这个桶**有 L4 副本，返回一个随机副本 key 与 true。
// 粒度 = 桶（注册字段 r4:{源桶key}，v7.0）。
// 副本死（过期未续）由读侧同 pipeline EXISTS 判定回退源桶（exec 承担）。
func (m *L4Manager) ReplicaFor(ctx context.Context, table, idxID, srcBucketKey string, randFn func(int) int) (string, bool) {
	fk := table + "/" + idxID + "/" + l4Field(srcBucketKey)
	// 1s 读缓存：陈旧窗口的误重定向由读侧 EXISTS 回退源桶兜住（正确性不依赖缓存新鲜度）
	m.repMu.RLock()
	if e, ok := m.repCache[fk]; ok && time.Since(e.at) < time.Second {
		k := e.k
		m.repMu.RUnlock()
		if k <= 0 {
			return "", false
		}
		return keycodec.ReplicaKey(srcBucketKey, 1+randFn(k)), true
	}
	m.repMu.RUnlock()

	res, err := m.cli.Do(ctx, "HGET", keycodec.BucketMapHotKey(table, idxID), l4Field(srcBucketKey))
	k := 0
	if err == nil && res != nil {
		k, _ = strconv.Atoi(fmt.Sprint(res))
	}
	m.repMu.Lock()
	m.repCache[fk] = repEnt{k: k, at: time.Now()}
	m.repMu.Unlock()
	if k <= 0 {
		return "", false
	}
	return keycodec.ReplicaKey(srcBucketKey, 1+randFn(k)), true
}
