package gateway

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
)

// slowlog.go：慢查询日志（docs/10 §10.4）——超阈值查询记录完整执行摘要：
// 语句指纹（NormalizeDigest）、路由、行数、耗时、是否全扫。
// 无索引谓词走了全扫放行的（hint/白名单）**强制告警**（与阈值无关）+
// fullscan_fallback_total 计数。

// slowQuery 在 ComQuery 结束时统一判定（defer 调用）。
func (h *kidbHandler) slowQuery(ctx context.Context, query, route string, fullscan bool, rows int, dur time.Duration, qerr error) {
	threshold := h.slowThreshold(ctx)
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

// slowThreshold 读 slow_query_threshold_ms 变量（默认 500ms；本地缓存秒级）。
func (h *kidbHandler) slowThreshold(ctx context.Context) time.Duration {
	v, _, err := h.s.cfg.Get(ctx, "slow_query_threshold_ms")
	if err != nil {
		return 500 * time.Millisecond
	}
	ms, err := strconv.Atoi(v)
	if err != nil || ms < 0 {
		return 500 * time.Millisecond
	}
	return time.Duration(ms) * time.Millisecond
}
