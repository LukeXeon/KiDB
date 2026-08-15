package controller

import (
	"context"

	"kidb"
	"kidb/bucketmap"
	"kidb/keycodec"
	"kidb/telemetry"
)

// autosplit.go：Controller 自治决策循环（docs/08 §8.1/§8.2）：
// 候选桶（遥测采样登记）→ ZCARD 精确复核 → 超阈分裂 / 高热建 L4 副本。

// Manager 是自治决策器（由选举产生的 Controller 驱动 Tick）。
type Manager struct {
	cli      kidb.KvClient
	bm       *bucketmap.Store
	splitter *Splitter
	l4       *L4Manager

	SplitMembers int64 // bucket_split_members 默认 50000
	HotQPS       int64 // 单桶读 QPS 热阈（简化：每 tick ops 增量）
}

// NewManager 构造。
func NewManager(cli kidb.KvClient, bm *bucketmap.Store, splitter *Splitter, l4 *L4Manager) *Manager {
	return &Manager{cli: cli, bm: bm, splitter: splitter, l4: l4, SplitMembers: 50000, HotQPS: 4000}
}

// Tick 一轮决策：候选复核 → 分裂/L4。错误不致命（下轮再来）。
func (m *Manager) Tick(ctx context.Context) error {
	cands, err := telemetry.Candidates(ctx, m.cli)
	if err != nil {
		return err
	}
	for _, bk := range cands {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		m.review(ctx, bk)
	}
	return nil
}

// review 复核单桶并动作。
func (m *Manager) review(ctx context.Context, bucketKey string) {
	table, idxID, encVal, slot, _, ok := keycodec.ParseEqBucketKey(bucketKey)
	if !ok {
		// 范围桶候选（i:{t}:{idx}:{stag}#r{n}）
		t2, i2, sl2, rangeSub, ok2 := keycodec.ParseRangeBucketKey(bucketKey)
		if !ok2 {
			_, _ = m.cli.Do(ctx, "HDEL", telemetry.CandKey, bucketKey)
			return
		}
		n, err := telemetry.Confirm(ctx, m.cli, bucketKey)
		if err != nil || n < m.SplitMembers {
			if err == nil {
				_, _ = m.cli.Do(ctx, "HDEL", telemetry.CandKey, bucketKey)
			}
			return
		}
		_ = m.splitter.SplitRange(ctx, t2, i2, sl2, rangeSub) // 失败下轮再来
		return
	}
	n, err := telemetry.Confirm(ctx, m.cli, bucketKey)
	if err != nil {
		return
	}
	if n >= m.SplitMembers {
		_ = m.splitter.SplitEq(ctx, table, idxID, encVal, slot) // 失败下轮再来
		return
	}
	// L4：高热但未达分裂阈（读 QPS 型热点）建副本摊开读
	if n >= 1000 { // 有规模的热值桶才值得副本（L4 不为小桶付成本）
		_ = m.l4.Activate(ctx, table, idxID, encVal, bucketKey, 2)
	}
}
