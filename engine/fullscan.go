package engine

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"kidb"
	"kidb/i18n"
	"kidb/metrics"
	"kidb/tuning"
)

// fullscan.go：引擎层全表遍历闸门（docs/07 §7.4 访问控制 + docs/04 §4.4）。
// 无索引谓词纪律与 JOIN 有界性裁决的唯一落点——任何引擎层全扫先过闸：
//
//  1. 小表（实时行数 < tuning gateway.dimension_max_rows）自动放行
//     （与维表广播同源，自动即可，docs/01 §1.0）；
//  2. 表白名单（query_allow_fullscan_tables 全局变量）放行并告警 + 指标；
//  3. 否则 ERR_NO_INDEX（附建索引/白名单建议）。

// NewFullscanGate 装配闸门（gateway/wire 注入 Deps.FullscanGate）。
// m 可为 nil（无指标环境，如测试）。
func NewFullscanGate(m *metrics.Metrics) func(*sql.Context, string, uint64) error {
	return func(ctx *sql.Context, table string, rows uint64) error {
		if rows < uint64(tuning.Get().Gateway.DimensionMaxRows) {
			return nil // 小表自动放行
		}
		if allowlistHas(SysvarString(VarFullscanAllowlist), table) {
			if m != nil {
				m.FullscanTotal.Inc()
			}
			slog.Warn(i18n.T("log.fullscan_allowlisted"), "table", table, "rows", rows)
			return nil
		}
		return fmt.Errorf("%w: %s", kidb.ErrNoIndex, i18n.T("err.no_index", table, rows))
	}
}

// allowlistHas 表白名单匹配（逗号分隔，大小写不敏感——GetTableInsensitive 同款口径）。
func allowlistHas(list, table string) bool {
	for _, e := range strings.Split(list, ",") {
		if strings.EqualFold(strings.TrimSpace(e), table) {
			return true
		}
	}
	return false
}
