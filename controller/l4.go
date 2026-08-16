// Package controller 的 L4 热桶副本管理（docs/08 §8.4）：
// 超阈自动建 K 个异 slot 只读副本（stag 步进替换），1s 滚动刷新，热度回落自动回收。
// 最终一致窗口（≤刷新周期）：副本内行级新增/变更在窗口内不可见——
// 与异步索引/副本读同级的文档化语义（docs/14 §14.2 最终一致窗口）。
package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"kidb"
	"kidb/utils"
	"kidb/bucketmap"
	"kidb/keycodec"
	"kidb/script"
)

// L4Manager 管理热桶副本。
type L4Manager struct {
	cli     kidb.KvClient
	reg     *script.Registry
	maxRep  int           // hotkey_replica_max（默认 8）
	ttlMs   int64         // 副本 TTL（60s 滚动）
	refresh time.Duration // 刷新周期（1s）
}

// NewL4 构造。
func NewL4(cli kidb.KvClient, reg *script.Registry) *L4Manager {
	return &L4Manager{cli: cli, reg: reg, maxRep: 8, ttlMs: 60000, refresh: time.Second}
}

// ReplicaVerField 是副本 Hash 里的版本戳字段。
const ReplicaVerField = "@ver"

// l4Field 注册表字段名（bmh:{table}:{idx} 内）。
func l4Field(encVal string) string { return "r4:" + encVal }

// Activate 为热桶建 K 个副本（K=⌈热QPS/单节点安全QPS⌉ ≤ maxRep）。
// srcBucketKey 必须含 stag；副本 key 由 keycodec.ReplicaKey 步进替换生成。
func (m *L4Manager) Activate(ctx context.Context, table, idxID, encVal, srcBucketKey string, k int) error {
	if k <= 0 {
		return nil
	}
	if k > m.maxRep {
		k = m.maxRep
	}
	if err := m.Refresh(ctx, srcBucketKey, k); err != nil {
		return err
	}
	// 注册：读路径据此切换到副本（bmh 注册表字段 r4:{encVal} = K）
	res, err := m.cli.Do(ctx, "HGET", bucketmap.RegistryKey(table, idxID), l4Field(encVal))
	if err != nil {
		return err
	}
	if res != nil {
		return nil // 已激活
	}
	_, err = m.cli.Do(ctx, "HSET", bucketmap.RegistryKey(table, idxID), l4Field(encVal), strconv.Itoa(k))
	return err
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
	ver := strconv.FormatInt(time.Now().UnixNano(), 10)
	for ri := 1; ri <= k; ri++ {
		argv := []any{ver, strconv.FormatInt(m.ttlMs, 10), strconv.Itoa(len(arr) / 2)}
		for i := 0; i+1 < len(arr); i += 2 {
			argv = append(argv, fmt.Sprint(arr[i+1]), fmt.Sprint(arr[i])) // score, member
		}
		if _, err := m.cli.Eval(ctx, rf, []string{keycodec.ReplicaKey(srcBucketKey, ri)}, argv...); err != nil {
			return err
		}
	}
	return nil
}

// Deactivate 回收副本（热度回落）：注销注册表 + UNLINK 全部副本。
func (m *L4Manager) Deactivate(ctx context.Context, table, idxID, encVal, srcBucketKey string) error {
	res, err := m.cli.Do(ctx, "HGET", bucketmap.RegistryKey(table, idxID), l4Field(encVal))
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
	_, err = m.cli.Do(ctx, "HDEL", bucketmap.RegistryKey(table, idxID), l4Field(encVal))
	return err
}

// ReplicaFor 读路径入口：若该值有 L4 副本，返回一个随机副本 key 与 true。
// 副本过期（60s 未续期）自动失效——读不到成员时回退源桶由调用方兜底
// （docs/08 §8.4：副本过期自动失效，读回退源桶）。
func (m *L4Manager) ReplicaFor(ctx context.Context, table, idxID, encVal, srcBucketKey string, randFn func(int) int) (string, bool) {
	res, err := m.cli.Do(ctx, "HGET", bucketmap.RegistryKey(table, idxID), l4Field(encVal))
	if err != nil || res == nil {
		return "", false
	}
	k, _ := strconv.Atoi(fmt.Sprint(res))
	if k <= 0 {
		return "", false
	}
	return keycodec.ReplicaKey(srcBucketKey, 1+randFn(k)), true
}
