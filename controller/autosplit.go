package controller

import (
	"context"

	"kidb"
	"kidb/bucketmap"
	"kidb/keycodec"
	"kidb/meta"
	"kidb/metrics"
	"kidb/telemetry"
	"kidb/tuning"
)

// autosplit.go：Controller 自治决策循环（docs/08 §8.1/§8.2）：
// 候选桶（遥测采样登记）→ ZCARD 精确复核 → 超阈分裂 / 高热建 L4 副本；
// L4 生命周期（刷新续期/冷却回收）随 Tick 驱动。
//
// 冷却面纪律：复核过的候选一律摘除注册（eq 路径曾经不 HDEL——
// 候选注册表无限增长且每 tick 全量复核陈旧项，review 实证）。

// Manager 是自治决策器（由选举产生的 Controller 驱动 Tick）。
type Manager struct {
	cli      kidb.KvClient
	bm       *bucketmap.Store
	splitter *Splitter
	l4       *L4Manager
	store    *meta.CatalogStore // L4 生命周期巡检的表清单源
	m        *metrics.Metrics   // nil = no-op（index_bucket_members 复核实测）

	SplitMembers int64 // 桶分裂成员阈值（tuning.toml controller.split_members）
	L4Members    int64 // L4 激活成员阈值（内置推导：SplitMembers/50）
}

// NewManager 构造。
func NewManager(cli kidb.KvClient, bm *bucketmap.Store, splitter *Splitter, l4 *L4Manager, store *meta.CatalogStore, m *metrics.Metrics) *Manager {
	sm := tuning.Get().Controller.SplitMembers
	return &Manager{cli: cli, bm: bm, splitter: splitter, l4: l4, store: store, m: m, SplitMembers: sm, L4Members: max(sm/50, 1)}
}

// Tick 一轮决策：L4 生命周期巡检 → 候选复核 → 分裂/L4。错误不致命（下轮再来）。
func (m *Manager) Tick(ctx context.Context) error {
	if err := m.l4.Tick(ctx, m.store); err != nil {
		return err
	}
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

// review 复核单桶并动作（复核结论落定即摘除候选——无论分裂/L4/不够格）。
func (m *Manager) review(ctx context.Context, bucketKey string) {
	defer func() { _, _ = m.cli.Do(ctx, "HDEL", telemetry.CandKey, bucketKey) }()

	table, idxID, encVal, slot, _, ok := keycodec.ParseEqBucketKey(bucketKey)
	if !ok {
		// 范围桶候选（i:{t}:{idx}:{stag}#r{n}）
		t2, i2, sl2, rangeSub, ok2 := keycodec.ParseRangeBucketKey(bucketKey)
		if !ok2 {
			return
		}
		n, err := telemetry.Confirm(ctx, m.cli, bucketKey)
		if err == nil && m.m != nil {
			m.m.BucketMembers.WithLabelValues(t2, i2).Set(float64(n))
		}
		if err != nil || n < m.SplitMembers {
			return
		}
		_ = m.splitter.SplitRange(ctx, t2, i2, sl2, rangeSub) // 失败下轮再登记（遥测重采样）
		return
	}
	n, err := telemetry.Confirm(ctx, m.cli, bucketKey)
	if err == nil && m.m != nil {
		m.m.BucketMembers.WithLabelValues(table, idxID).Set(float64(n))
	}
	if err != nil {
		return
	}
	if n >= m.SplitMembers {
		_ = m.splitter.SplitEq(ctx, table, idxID, encVal, slot)
		return
	}
	// L4：高热但未达分裂阈（读 QPS 型热点）建副本摊开读
	if n >= m.L4Members {
		_ = m.l4.Activate(ctx, table, idxID, encVal, slot, bucketKey, 2)
	}
}
