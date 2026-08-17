// Package metrics 是 KiDB 指标体系（docs/10 §10.3）：
// prometheus 系列经 Registerer 注入（可替换后端）；nil Metrics = no-op（零成本关闭）。
// 纪律（review 实证）：注册即接线——只保留有真实写入点的系列（注册即死 = cnt 同款 vestigial）。
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
	SweptTotal      prometheus.Counter       // swept_total
	NearcacheHits   prometheus.Counter       // nearcache 命中
	NearcacheMiss   prometheus.Counter       // nearcache 未命中
	RowsFiltered    prometheus.Counter       // rowiter_rows_filtered_total（回表校验拦截量）
	FullscanTotal   prometheus.Counter       // fullscan_fallback_total
	LuaStaleRetry   prometheus.Counter       // lua_stale_retry_total
	ConfigSet       *prometheus.CounterVec   // config_set_total{result}
	LeaseRefresh    prometheus.Counter       // schema_lease_refresh_total
	DDLJobDuration  *prometheus.HistogramVec // ddl_job_duration_seconds{type}
	OwnerTransition prometheus.Counter       // owner_role_transitions_total
	ReconcileDrift  *prometheus.CounterVec   // reconcile_drift_total{kind}（对账漂移，docs/12 §12.8；正常=0）
	RoleConcede     *prometheus.CounterVec   // role_concede_total{reason}（任职退让，docs/08 §8.5）
	RoleVacancy     prometheus.Gauge         // role_vacancy_seconds（锁连续空窗时长；>0 即无人接管）
	IndexDLQ        prometheus.Counter       // index_dlq_total（异步补写死信条目，docs/12 §12.8）
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
		SweptTotal:    prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "swept_total"}),
		NearcacheHits: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "nearcache_hits_total"}),
		NearcacheMiss: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "nearcache_miss_total"}),
		RowsFiltered:  prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "rowiter_rows_filtered_total"}),
		FullscanTotal: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "fullscan_fallback_total"}),
		LuaStaleRetry: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "lua_stale_retry_total"}),
		ConfigSet: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "kidb", Name: "config_set_total",
		}, []string{"result"}),
		LeaseRefresh: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "schema_lease_refresh_total"}),
		DDLJobDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "kidb", Name: "ddl_job_duration_seconds", Buckets: prometheus.DefBuckets,
		}, []string{"type"}),
		OwnerTransition: prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "owner_role_transitions_total"}),
		ReconcileDrift: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "kidb", Name: "reconcile_drift_total",
		}, []string{"kind"}),
		RoleConcede: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "kidb", Name: "role_concede_total",
		}, []string{"reason"}),
		RoleVacancy: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "kidb", Name: "role_vacancy_seconds"}),
		IndexDLQ:    prometheus.NewCounter(prometheus.CounterOpts{Namespace: "kidb", Name: "index_dlq_total"}),
	}
	reg.MustRegister(
		m.QueryDuration, m.ScatterFanout, m.BucketMembers, m.Splits, m.Merges,
		m.HotReplicas, m.SweptTotal, m.NearcacheHits, m.NearcacheMiss,
		m.RowsFiltered, m.FullscanTotal, m.LuaStaleRetry,
		m.ConfigSet, m.LeaseRefresh, m.DDLJobDuration,
		m.OwnerTransition, m.ReconcileDrift, m.RoleConcede, m.RoleVacancy, m.IndexDLQ,
	)
	return m
}
