package gateway

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"

	"kidb"
	"kidb/meta"
)

// sqlguard.go：DML 有界性执法（docs/04 §4.1/§4.4 的落实点）：
//   - 无索引谓词 → ERR_NO_INDEX（FULLSCAN hint 或白名单放行，docs/07 §7.4）；
//   - 档 4 大表任意 JOIN → ERR_UNSUPPORTED_JOIN（档 1 主键查找/档 2 维表广播放行）。
//
// 识别用 TiDB parser（与分类器分工：分类器管 DDL/DML 路由，本组件管 DML 有界性）。
// 判不准一律放行给引擎（引擎通用路径正确，只是可能慢——执法收紧不依赖猜测）。

// enforceQueryPolicy 对引擎路径语句执法；nil = 放行。
func (h *kidbHandler) enforceQueryPolicy(ctx context.Context, query string) error {
	// 快筛：只有 SELECT/UPDATE/DELETE 需要判定
	words := leadingWords(stripComments(query), 1)
	if len(words) == 0 {
		return nil
	}
	switch words[0] {
	case "SELECT", "UPDATE", "DELETE":
	default:
		return nil
	}

	stmts, _, err := parser.New().Parse(query, "", "")
	if err != nil || len(stmts) != 1 {
		return nil // 解析不了交给引擎报错
	}

	var sel *ast.SelectStmt
	var where ast.ExprNode
	switch s := stmts[0].(type) {
	case *ast.SelectStmt:
		sel = s
		where = s.Where
	case *ast.UpdateStmt:
		where = s.Where
		if s.TableRefs != nil && s.TableRefs.TableRefs != nil && s.TableRefs.TableRefs.Right != nil {
			return fmt.Errorf("%w: 多表 UPDATE", kidb.ErrUnsupported)
		}
	case *ast.DeleteStmt:
		where = s.Where
		if s.IsMultiTable {
			return fmt.Errorf("%w: 多表 DELETE", kidb.ErrUnsupported)
		}
	default:
		return nil
	}

	// JOIN 分档（过了档位判定即为有界查询，不再叠加无索引检查——
	// 档 1/2 的代价上界由档位语义保证，docs/04 §4.4）
	if sel != nil && sel.From != nil && sel.From.TableRefs != nil && sel.From.TableRefs.Right != nil {
		return h.checkJoins(ctx, sel)
	}

	// 无索引谓词执法（无 WHERE = 全表遍历，同纪律）
	return h.checkBoundedScan(ctx, stmts[0], where, query)
}

// checkJoins 逐 JOIN 判定档位（档 1/2 放行，档 4 报错）。
func (h *kidbHandler) checkJoins(ctx context.Context, sel *ast.SelectStmt) error {
	if sel.From == nil || sel.From.TableRefs == nil || sel.From.TableRefs.Right == nil {
		return nil // 无 JOIN
	}
	return h.checkJoinNode(ctx, sel.From.TableRefs)
}

// checkJoinNode 递归左深 Join 树。
func (h *kidbHandler) checkJoinNode(ctx context.Context, j *ast.Join) error {
	if l, ok := j.Left.(*ast.Join); ok {
		if err := h.checkJoinNode(ctx, l); err != nil {
			return err
		}
	}
	// 右表必须具名（子查询右枝不做档 1/2 判定 → 档 4）
	ts, ok := j.Right.(*ast.TableSource)
	if !ok {
		return fmt.Errorf("%w: 子查询作为 JOIN 右枝", kidb.ErrUnsupportedJoin)
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return fmt.Errorf("%w: JOIN 右枝非表", kidb.ErrUnsupportedJoin)
	}
	rightName := tn.Name.O
	if ts.AsName.O != "" {
		rightName = ts.AsName.O
	}

	if j.On == nil {
		return fmt.Errorf("%w: CROSS JOIN 无界", kidb.ErrUnsupportedJoin)
	}
	// ON 等值条件中的右表列集合
	eqCols := map[string]bool{}
	collectOnEqCols(j.On.Expr, rightName, eqCols)

	def, err := h.s.deps.Cache.Get(ctx, tn.Name.O)
	if err != nil {
		return err
	}
	if def == nil {
		return fmt.Errorf("%w: 表 %q 不存在", kidb.ErrUnsupported, tn.Name.O)
	}
	// 档 1：右表主键出现在 ON 等值中
	if eqCols[strings.ToLower(def.PK)] {
		return nil
	}
	// 档 2：维表广播（dimension 标记 + 行数 < 10 万）
	if def.Dimension {
		n, err := h.s.deps.Exec.RowCount(ctx, def, time.Now().Unix())
		if err == nil && n < 100000 {
			return nil
		}
	}
	return fmt.Errorf("%w: %s 非主键等值关联（档 4 无界 JOIN）", kidb.ErrUnsupportedJoin, rightName)
}

// collectOnEqCols 收集 ON 条件里 `... = rightAlias.col` 形态的右表列名。
func collectOnEqCols(e ast.ExprNode, rightAlias string, out map[string]bool) {
	be, ok := e.(*ast.BinaryOperationExpr)
	if !ok {
		return
	}
	switch be.Op {
	case opcode.LogicAnd:
		collectOnEqCols(be.L, rightAlias, out)
		collectOnEqCols(be.R, rightAlias, out)
	case opcode.EQ:
		for _, side := range []ast.ExprNode{be.L, be.R} {
			if ce, ok := side.(*ast.ColumnNameExpr); ok &&
				strings.EqualFold(ce.Name.Table.O, rightAlias) {
				out[strings.ToLower(ce.Name.Name.O)] = true
			}
		}
	}
}

// checkBoundedScan 无索引谓词执法。
func (h *kidbHandler) checkBoundedScan(ctx context.Context, stmt ast.StmtNode, where ast.ExprNode, rawQuery string) error {
	table := tableOf(stmt)
	if table == "" {
		return nil
	}
	def, err := h.s.deps.Cache.Get(ctx, table)
	if err != nil || def == nil {
		return err
	}
	if where == nil {
		// ORDER BY 首个排序列有可用范围索引 → 有界（全局 score 有序流，
		// LIMIT 早停由引擎停止消费达成，docs/04 §4.1 top-k）
		if sel, ok := stmt.(*ast.SelectStmt); ok && orderByHasRangeIndex(sel, def) {
			return nil
		}
		// 无 WHERE 全表遍历：白名单或 hint 放行
		return h.allowFullscan(ctx, def, rawQuery)
	}
	cols := map[string]bool{}
	collectPredCols(where, cols)
	// 前缀 LIKE 的列单独收集（PatternLikeExpr 非比较运算，不进 cols）
	prefixCols := map[string]bool{}
	collectPrefixLikeCols(where, prefixCols)
	building := ""
	for c := range cols {
		switch indexStateOn(def, c) {
		case 2:
			return nil // 有可用索引谓词 → 放行
		case 1:
			building = c
		}
	}
	for c := range prefixCols {
		switch prefixIndexStateOn(def, c) {
		case 2:
			return nil // 有字典序副本 → 前缀搜索路径（docs/04 §4.5）
		case 1:
			building = c
		}
	}
	if building != "" {
		return fmt.Errorf("%w: 表 %s 列 %s 的索引建设中，稍后重试（在线回填，docs/06 §6.3）", kidb.ErrNoIndex, def.Name, building)
	}
	return h.allowFullscan(ctx, def, rawQuery)
}

// collectPrefixLikeCols 收集常量前缀 LIKE 的列（`col LIKE 'abc%'`：
// 恰一个 % 结尾、无 _/转义、前缀非空——其余形态不收集，走全扫纪律）。
func collectPrefixLikeCols(e ast.ExprNode, out map[string]bool) {
	switch x := e.(type) {
	case *ast.BinaryOperationExpr:
		if x.Op == opcode.LogicAnd {
			collectPrefixLikeCols(x.L, out)
			collectPrefixLikeCols(x.R, out)
		}
	case *ast.PatternLikeOrIlikeExpr:
		if x.Not {
			return // NOT LIKE 不进前缀通道（保守走全扫纪律）
		}
		ce, ok := x.Expr.(*ast.ColumnNameExpr)
		if !ok {
			return
		}
		ve, ok := x.Pattern.(ast.ValueExpr)
		if !ok {
			return
		}
		pat, ok := ve.GetValue().(string)
		if !ok || len(pat) < 2 || !strings.HasSuffix(pat, "%") {
			return
		}
		prefix := pat[:len(pat)-1]
		if prefix == "" || strings.ContainsAny(prefix, "%_\\") {
			return
		}
		out[strings.ToLower(ce.Name.Name.O)] = true
	}
}

// prefixIndexStateOn 列上字典序副本状态：0=无，1=回填中，2=可用。
func prefixIndexStateOn(def *meta.TableDef, col string) int {
	state := 0
	for _, idx := range def.Indexes {
		if idx.PrefixCopy && len(idx.Columns) == 1 && strings.EqualFold(idx.Columns[0], col) {
			if idx.Building {
				state = 1
			} else if state == 0 {
				state = 2
			}
		}
	}
	return state
}

// allowFullscan 全表遍历访问控制（docs/07 §7.4：hint 或表白名单）。
func (h *kidbHandler) allowFullscan(ctx context.Context, def *meta.TableDef, rawQuery string) error {
	// hint 通道：/*+ FULLSCAN */（大小写不敏感，允许空白变形）
	q := strings.ToLower(strings.ReplaceAll(rawQuery, " ", ""))
	if strings.Contains(q, "/*+fullscan*/") {
		return nil
	}
	v, _, err := h.s.cfg.Get(ctx, "query_allow_fullscan_tables")
	if err == nil {
		for _, name := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(name), def.Name) {
				return nil
			}
		}
	}
	return fmt.Errorf("%w: 表 %s 无可用索引（建索引，或 /*+ FULLSCAN */ hint，或 SET GLOBAL query_allow_fullscan_tables）", kidb.ErrNoIndex, def.Name)
}

// indexStateOn 列上索引状态：0=无，1=回填中，2=可用。
func indexStateOn(def *meta.TableDef, col string) int {
	if strings.EqualFold(def.PK, col) {
		return 2
	}
	state := 0
	for _, idx := range def.Indexes {
		if len(idx.Columns) == 1 && strings.EqualFold(idx.Columns[0], col) {
			if idx.Building {
				state = 1
			} else if state == 0 {
				state = 2
			}
		}
	}
	return state
}

// collectPredCols 收集 AND 树中的谓词列（顶层合取）。
// 叶子形态：比较运算 / IN / BETWEEN（列引用取自表达式左侧）。
// OR 及更复杂的表达式保守不收集（宁可误拒走全扫纪律，不可误放）。
func collectPredCols(e ast.ExprNode, out map[string]bool) {
	switch x := e.(type) {
	case *ast.BinaryOperationExpr:
		switch x.Op {
		case opcode.LogicAnd:
			collectPredCols(x.L, out)
			collectPredCols(x.R, out)
		default: // 比较类叶子
			for _, side := range []ast.ExprNode{x.L, x.R} {
				if ce, ok := side.(*ast.ColumnNameExpr); ok {
					out[strings.ToLower(ce.Name.Name.O)] = true
				}
			}
		}
	case *ast.PatternInExpr: // col IN (...) / col NOT IN (...)
		if ce, ok := x.Expr.(*ast.ColumnNameExpr); ok {
			out[strings.ToLower(ce.Name.Name.O)] = true
		}
	case *ast.BetweenExpr: // col BETWEEN a AND b
		if ce, ok := x.Expr.(*ast.ColumnNameExpr); ok {
			out[strings.ToLower(ce.Name.Name.O)] = true
		}
	}
}

// orderByHasRangeIndex 判定 ORDER BY 首列是否有可用范围索引（回填中不算）。
// 仅识别简单列引用；表达式/序号排序保守返回 false。
func orderByHasRangeIndex(sel *ast.SelectStmt, def *meta.TableDef) bool {
	if sel.OrderBy == nil || len(sel.OrderBy.Items) == 0 {
		return false
	}
	ce, ok := sel.OrderBy.Items[0].Expr.(*ast.ColumnNameExpr)
	if !ok {
		return false
	}
	col := ce.Name.Name.O
	for _, idx := range def.Indexes {
		if idx.Kind == meta.IndexRange && !idx.Building &&
			len(idx.Columns) == 1 && strings.EqualFold(idx.Columns[0], col) {
			return true
		}
	}
	return false
}

// tableOf 取语句的主表名。
func tableOf(stmt ast.StmtNode) string {
	var ts *ast.TableSource
	switch s := stmt.(type) {
	case *ast.SelectStmt:
		if s.From == nil || s.From.TableRefs == nil {
			return ""
		}
		ts, _ = s.From.TableRefs.Left.(*ast.TableSource)
	case *ast.UpdateStmt:
		ts, _ = s.TableRefs.TableRefs.Left.(*ast.TableSource)
	case *ast.DeleteStmt:
		ts, _ = s.TableRefs.TableRefs.Left.(*ast.TableSource)
	}
	if ts == nil {
		return ""
	}
	if tn, ok := ts.Source.(*ast.TableName); ok {
		return tn.Name.O
	}
	return ""
}
