package gateway

import (
	"context"
	"regexp"
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"

	"kidb/config"
)

// config_plane.go：配置管理面的拦截实现（docs/10 §10.2：SET/SHOW GLOBAL
// 落到 cfg:global 系统表——配置即数据，与数据面同协议）。

var setGlobalRe = regexp.MustCompile(`(?i)^\s*SET\s+GLOBAL\s+([a-z0-9_]+)\s*=\s*(.+?)\s*;?\s*$`)
var showVarsRe = regexp.MustCompile(`(?i)^\s*SHOW\s+(?:GLOBAL\s+)?VARIABLES\s*(?:LIKE\s+('([^']*)'|"([^"]*)"))?\s*;?\s*$`)

// handleConfigStmt 识别并处理配置语句；返回是否已接管。
// SET GLOBAL 我们的变量 → ConfigStore CAS；SHOW [GLOBAL] VARIABLES LIKE →
// 从配置存储服务（其余形态委托引擎）。
func (h *kidbHandler) handleConfigStmt(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) (bool, error) {
	if m := setGlobalRe.FindStringSubmatch(query); m != nil {
		name := strings.ToLower(m[1])
		if _, ok := config.Vars[name]; !ok {
			return false, nil // 非 KiDB 变量 → 引擎路径（gms 自己的 sysvars）
		}
		value := unquote(m[2])
		if err := h.s.cfg.Set(ctx, name, value); err != nil {
			return true, sqlErr(err)
		}
		return true, callback(&sqltypes.Result{}, false)
	}

	if m := showVarsRe.FindStringSubmatch(query); m != nil {
		pat := m[2]
		if pat == "" {
			pat = m[3]
		}
		if pat == "" {
			return false, nil // 无 LIKE 的全量列举委托引擎（v1 缺口：KiDB 变量不出现在无模式列举）
		}
		rows, err := h.showVarsMatching(ctx, pat)
		if err != nil {
			return true, sqlErr(err)
		}
		return true, callback(rows, false)
	}
	return false, nil
}

// showVarsMatching 从配置存储生成 SHOW VARIABLES 结果集。
func (h *kidbHandler) showVarsMatching(ctx context.Context, pattern string) (*sqltypes.Result, error) {
	all, err := h.s.cfg.All(ctx)
	if err != nil {
		return nil, err
	}
	fields := []*querypb.Field{
		{Name: "Variable_name", Type: querypb.Type_VARCHAR},
		{Name: "Value", Type: querypb.Type_VARCHAR},
	}
	res := &sqltypes.Result{Fields: fields}
	for name, val := range all {
		if likeMatch(pattern, name) {
			res.Rows = append(res.Rows, []sqltypes.Value{
				sqltypes.NewVarChar(name), sqltypes.NewVarChar(val),
			})
		}
	}
	return res, nil
}

// likeMatch 最小 SQL LIKE 匹配（% 与 _）。
func likeMatch(pattern, s string) bool {
	// 简单递归（变量名很短，无回溯爆炸）
	plen, slen := len(pattern), len(s)
	memo := map[[2]int]bool{}
	seen := map[[2]int]bool{}
	var match func(pi, si int) bool
	match = func(pi, si int) bool {
		key := [2]int{pi, si}
		if seen[key] {
			return memo[key]
		}
		seen[key] = true
		var res bool
		switch {
		case pi == plen:
			res = si == slen
		case pattern[pi] == '\\' && pi+1 < plen: // 转义：\_ \% \\ 字面
			res = si < slen && pattern[pi+1] == s[si] && match(pi+2, si+1)
		case pattern[pi] == '%':
			res = match(pi+1, si) || (si < slen && match(pi, si+1))
		case si < slen && (pattern[pi] == '_' || pattern[pi] == s[si]):
			res = match(pi+1, si+1)
		}
		memo[key] = res
		return res
	}
	return match(0, 0)
}

// unquote 去掉字符串引号。
func unquote(v string) string {
	if len(v) >= 2 {
		if (v[0] == '\'' && v[len(v)-1] == '\'') || (v[0] == '"' && v[len(v)-1] == '"') {
			return v[1 : len(v)-1]
		}
	}
	return v
}
