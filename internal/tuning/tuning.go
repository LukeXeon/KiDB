// Package tuning 是开发者调优参数的唯一入口（docs/01 §1.0 设计原点：
// 面向用户的变量只有 cfg:global 的 3 个语义开关；开发者参数统一收进
// tuning.toml，go:embed 进二进制，改参数需重新构建发布——不热更）。
//
// 用法：tuning.Get().Exec.Batch（启动期加载并校验，解析失败直接 panic——
// 文件随二进制发布，解析失败 = 构建期 bug，由测试拦截）。
// 测试调整：tuning.OverrideForTest(t, fn) 在测试内替换并自动还原。
package tuning

import (
	_ "embed"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pelletier/go-toml/v2"
)

//go:embed tuning.toml
var embedded []byte

// Tuning 是 tuning.toml 的映射结构（层级与文件一致）。
type Tuning struct {
	Gateway struct {
		SlowQueryThresholdMs int `toml:"slow_query_threshold_ms"`
		PlanCacheCapacity    int `toml:"plan_cache_capacity"`
		DimensionMaxRows     int `toml:"dimension_max_rows"`
	} `toml:"gateway"`
	Nearcache struct {
		TTLMs       int `toml:"ttl_ms"`
		Capacity    int `toml:"capacity"`
		RowTTLMs    int `toml:"row_ttl_ms"`
		RowCapacity int `toml:"row_capacity"`
	} `toml:"nearcache"`
	Exec struct {
		Batch               int `toml:"batch"`
		SlotsPerRound       int `toml:"slots_per_round"`
		FullscanConcurrency int `toml:"fullscan_concurrency"`
		L2MaxCollect        int `toml:"l2_max_collect"`
		TopkRefillPage      int `toml:"topk_refill_page"`
		LexRefillPage       int `toml:"lex_refill_page"`
	} `toml:"exec"`
	Controller struct {
		SplitMembers        int64   `toml:"split_members"`
		SplitBytes          int64   `toml:"split_bytes"`
		SplitQPSRatio       float64 `toml:"split_qps_ratio"`
		MergeMembers        int64   `toml:"merge_members"`
		MergeSustainPeriods int     `toml:"merge_sustain_periods"`
		HotkeyReplicaMax    int     `toml:"hotkey_replica_max"`
		HotkeyRefreshMs     int     `toml:"hotkey_refresh_ms"`
		JobTickBudgetMs     int     `toml:"job_tick_budget_ms"`
		JobSlotsPerTick     int     `toml:"job_slots_per_tick"`
		BackfillRowsPerSec  int     `toml:"backfill_rows_per_sec"`
		HotQPS              int64   `toml:"hot_qps"`
	} `toml:"controller"`
	Sweeper struct {
		Batch             int `toml:"batch"`
		MaxBatchesPerTick int `toml:"max_batches_per_tick"`
		ReceiptGraceMs    int `toml:"receipt_grace_ms"`
		TickMs            int `toml:"tick_ms"`
		SweepRangeSlots   int `toml:"sweep_range_slots"`
	} `toml:"sweeper"`
	Txguard struct {
		StaleRetries       int     `toml:"stale_retries"`
		AsyncLogCapacity   int     `toml:"async_log_capacity"`
		AsyncLogAlertRatio float64 `toml:"async_log_alert_ratio"`
		MaxRowBytes        int     `toml:"max_row_bytes"`
	} `toml:"txguard"`
	Telemetry struct {
		SampleRatio int `toml:"sample_ratio"`
	} `toml:"telemetry"`
	HLL struct {
		SampleRate int `toml:"sample_rate"`
	} `toml:"hll"`
	Meta struct {
		SchemaLeaseMs int `toml:"schema_lease_ms"`
	} `toml:"meta"`
	Retry struct {
		BackoffBaseMs int `toml:"backoff_base_ms"`
		BackoffMaxMs  int `toml:"backoff_max_ms"`
		MaxAttempts   int `toml:"max_attempts"`
	} `toml:"retry"`
}

var current atomic.Pointer[Tuning]

func init() {
	t, err := parse(embedded)
	if err != nil {
		panic(fmt.Sprintf("tuning: embedded tuning.toml 解析失败（构建期 bug）: %v", err))
	}
	current.Store(t)
}

func parse(data []byte) (*Tuning, error) {
	t := &Tuning{}
	if err := toml.Unmarshal(data, t); err != nil {
		return nil, err
	}
	return t.validate()
}

// validate 关键红线校验（fail-fast；启动即拒）。
func (t *Tuning) validate() (*Tuning, error) {
	if t.Controller.SplitBytes <= 0 || t.Controller.SplitBytes > 16<<20 {
		return nil, fmt.Errorf("controller.split_bytes %d 越界（≤16MB rebalance 红线）", t.Controller.SplitBytes)
	}
	if t.Exec.Batch <= 0 || t.Nearcache.Capacity <= 0 || t.Controller.SplitMembers <= 0 {
		return nil, fmt.Errorf("批大小/容量/分裂阈值必须为正")
	}
	if t.Telemetry.SampleRatio <= 0 || t.HLL.SampleRate <= 0 {
		return nil, fmt.Errorf("采样率必须为正")
	}
	return t, nil
}

// Get 返回当前调优参数（不可变；替换整体发生）。
func Get() *Tuning { return current.Load() }

// OverrideForTest 测试内替换参数（t.Cleanup 自动还原）。
func OverrideForTest(tb testing.TB, fn func(*Tuning)) {
	tb.Helper()
	old := current.Load()
	clone := *old
	fn(&clone)
	if _, err := clone.validate(); err != nil {
		tb.Fatalf("tuning override 非法: %v", err)
	}
	current.Store(&clone)
	tb.Cleanup(func() { current.Store(old) })
}

// ==== 便捷访问器（Duration 换算集中在此）====

// SlowQueryThreshold 慢查询阈值。
func (t *Tuning) SlowQueryThreshold() time.Duration {
	return time.Duration(t.Gateway.SlowQueryThresholdMs) * time.Millisecond
}

// NearcacheTTL L1 条目 TTL。
func (t *Tuning) NearcacheTTL() time.Duration {
	return time.Duration(t.Nearcache.TTLMs) * time.Millisecond
}

// NearcacheRowTTL 行级近缓存默认条目寿命。
func (t *Tuning) NearcacheRowTTL() time.Duration {
	return time.Duration(t.Nearcache.RowTTLMs) * time.Millisecond
}

// SchemaLease 元数据租约。
func (t *Tuning) SchemaLease() time.Duration {
	return time.Duration(t.Meta.SchemaLeaseMs) * time.Millisecond
}

// ReceiptGrace 回执宽限。
func (t *Tuning) ReceiptGrace() time.Duration {
	return time.Duration(t.Sweeper.ReceiptGraceMs) * time.Millisecond
}

// JobTickBudget DDL 回填每 tick 时间预算。
func (t *Tuning) JobTickBudget() time.Duration {
	return time.Duration(t.Controller.JobTickBudgetMs) * time.Millisecond
}

// HotkeyRefresh L4 副本刷新周期。
func (t *Tuning) HotkeyRefresh() time.Duration {
	return time.Duration(t.Controller.HotkeyRefreshMs) * time.Millisecond
}
