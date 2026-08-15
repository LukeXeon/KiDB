// Package metrics 是 KiDB 指标体系（docs/10 §10.3）：
// prometheus 系列经 Registerer 注入（可替换后端）；nil Metrics = no-op（零成本关闭）。
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics 是全部内核系列（docs/10 §10.3 命名即事实）。
type Metrics struct {
	QueryDuration   *prometheus.HistogramVec // query_duration_seconds{plan}
	ScatterFanout   prometheus.Histogram     // query_scatter_fanout
	BucketMembers   *prometheus.GaugeVec     // index_bucket_members{table,idx}
	Splits          prometheus.Counter       // index_bucket_splits_total
	Merges          prometheus.Counter       // index_bucket_merges_total
	HotReplicas     prometheus.Gauge         // hotkey_replicas_active
	SweeperLag      prometheus.Gauge         // sweeper_lag_rows
	SweptTotal      prometheus.Counter       // swept_total
	NearcacheHits   prometheus.Counter       // nearcache 命中
	NearcacheMiss   prometheus.Counter       // nearcache 未命中
	AsyncBacklog    prometheus.Gauge         // async_index_log_backlog
	RowsFiltered    prometheus.Counter       // rowiter_rows_filtered_total（回表校验拦截量）
	FullscanTotal   prometheus.Counter       // fullscan_fallback_total
	LuaStaleRetry   prometheus.Counter       // lua_stale_retry_total
	LuaNoscript     prometheus.Counter       // lua_noscript_total
	ConfigSet       *prometheus.CounterVec   // config_set_total{result}
	PlanCacheHit    prometheus.Counter       // plan cache 命中
	PlanCacheStale  prometheus.Counter       // plan_cache_stale_total（版本失效重建）
	LeaseRefresh    prometheus.Counter       // schema_lease_refresh_total
	DDLJobDuration  *prometheus.HistogramVec // ddl_job_duration_seconds{type}
	ContractViolate prometheus.Counter       // contract_violation_total（应为 0）
	OwnerTransition prometheus.Counter       // owner_role_transitions_total
	SlowQueries     *prometheus.CounterVec   // slow_queries_total{route}（docs/10 §10.4）
}

// New 注册全部系列；reg 为 nil 时用默认注册表。
func New(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	m := &Metrics{
		QueryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "kidb", Name: "query_duration_seconds", Buckets: prometheus.DefBuckets,
		}, []string{"plan"}),
		ScatterFanout: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "kidb", Name: "query_scatter_fanout", Buckets: []float64{1, 16, 64, 256, 1024, 4096, 16384},
		}),
		BucketMembers: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "kidb", Name: "index_bucket_members",
		}, []string{"table", "idx"}),
		Splits:        prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "index_bucket_splits_total"}),
		Merges:        prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "index_bucket_merges_total"}),
		HotReplicas:   prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "kidb", Name: "hotkey_replicas_active"}),
		SweeperLag:    prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "kidb", Name: "sweeper_lag_rows"}),
		SweptTotal:    prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "swept_total"}),
		NearcacheHits: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "nearcache_hits_total"}),
		NearcacheMiss: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "nearcache_miss_total"}),
		AsyncBacklog:  prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "kidb", Name: "async_index_log_backlog"}),
		RowsFiltered:  prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "rowiter_rows_filtered_total"}),
		FullscanTotal: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "fullscan_fallback_total"}),
		LuaStaleRetry: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "lua_stale_retry_total"}),
		LuaNoscript:   prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "lua_noscript_total"}),
		ConfigSet: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "kidb", Name: "config_set_total",
		}, []string{"result"}),
		PlanCacheHit:   prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "plan_cache_hits_total"}),
		PlanCacheStale: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "plan_cache_stale_total"}),
		LeaseRefresh:   prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "schema_lease_refresh_total"}),
		DDLJobDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "kidb", Name: "ddl_job_duration_seconds", Buckets: prometheus.DefBuckets,
		}, []string{"type"}),
		ContractViolate: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "contract_violation_total"}),
		OwnerTransition: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "owner_role_transitions_total"}),
		SlowQueries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "kidb", Name: "slow_queries_total",
		}, []string{"route"}),
	}
	reg.MustRegister(
		m.QueryDuration, m.ScatterFanout, m.BucketMembers, m.Splits, m.Merges,
		m.HotReplicas, m.SweeperLag, m.SweptTotal, m.NearcacheHits, m.NearcacheMiss,
		m.AsyncBacklog, m.RowsFiltered, m.FullscanTotal, m.LuaStaleRetry, m.LuaNoscript,
		m.ConfigSet, m.PlanCacheHit, m.PlanCacheStale, m.LeaseRefresh, m.DDLJobDuration,
		m.ContractViolate, m.OwnerTransition, m.SlowQueries,
	)
	return m
}
