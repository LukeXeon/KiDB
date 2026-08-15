package gateway

import (
	"context"
	"log/slog"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
)

// slowlog.go：慢查询日志（docs/10 §10.4）——超阈值查询记录完整执行摘要：
// 语句指纹（NormalizeDigest）、路由、行数、耗时、是否全扫。
// 无索引谓词走了全扫放行的（hint/白名单）**强制告警**（与阈值无关）+
// fullscan_fallback_total 计数。
//
// 阈值是进程级配置（Server.slowQueryThreshold，默认 500ms）——
// 不是共享变量（docs/01 §1.0：调优参数不设 SQL 变量）。

// slowQuery 在 ComQuery 结束时统一判定（defer 调用）。
func (h *kidbHandler) slowQuery(ctx context.Context, query, route string, fullscan bool, rows int, dur time.Duration, qerr error) {
	threshold := h.s.slowQueryThreshold
	slow := dur > threshold
	if !slow && !fullscan {
		return
	}
	// 指纹惰性计算（只在落日志时解析）
	_, digest := parser.NormalizeDigest(query)
	attrs := []any{
		"digest", digest,
		"route", route,
		"rows", rows,
		"dur_ms", dur.Milliseconds(),
		"threshold_ms", threshold.Milliseconds(),
	}
	if qerr != nil {
		attrs = append(attrs, "err", qerr.Error())
	}
	if fullscan {
		attrs = append(attrs, "fullscan", true)
		if m := h.s.deps.Exec.Metrics(); m != nil {
			m.FullscanTotal.Inc()
		}
		slog.Warn("kidb 全扫查询（hint/白名单放行）", attrs...)
		return
	}
	if m := h.s.deps.Exec.Metrics(); m != nil && m.SlowQueries != nil {
		m.SlowQueries.WithLabelValues(route).Inc()
	}
	slog.Warn("kidb 慢查询", attrs...)
}
