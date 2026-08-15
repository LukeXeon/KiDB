package gateway

import (
	"context"
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"

	"kidb"
	"kidb/meta"
)

// explain.go：EXPLAIN 的 KiDB 计划展示（docs/02 §2.8）。
//
// 形态：两列（item, detail）——列出命中索引/路径、桶与扇出估算、谓词下推、
// 覆盖命中、近缓存/副本标记、回表批数估算。只支持 EXPLAIN SELECT
// （其余形态报 1235——判不准不猜，宁缺毋错）。
//
// 这是**计划推断**（与执行同规则的 AST 分析），不是执行回放：
// 桶数/行数为结构估算（默认 16384 路扇出），不读数据。

// explainQuery 处理 EXPLAIN 语句；返回是否已接管。
func (h *kidbHandler) explainQuery(ctx context.Context, query string, callback mysql.ResultSpoolFn) (bool, error) {
	words := leadingWords(stripComments(query), 1)
	if len(words) == 0 || words[0] != "EXPLAIN" {
		return false, nil
	}
	plan, err := h.buildExplain(ctx, stripLeadingExplain(query))
	if err != nil {
		return true, sqlErr(err)
	}
	res := &sqltypes.Result{Fields: []*querypb.Field{
		{Name: "item", Type: querypb.Type_VARCHAR},
		{Name: "detail", Type: querypb.Type_VARCHAR},
	}}
	for _, row := range plan {
		res.Rows = append(res.Rows, []sqltypes.Value{sqltypes.NewVarChar(row[0]), sqltypes.NewVarChar(row[1])})
	}
	return true, callback(res, false)
}

// stripLeadingExplain 去掉首个 EXPLAIN 关键字（注释已剥过一次——这里原样处理空白）。
func stripLeadingExplain(query string) string {
	q := stripComments(query)
	i := strings.Index(strings.ToUpper(q), "EXPLAIN")
	if i < 0 {
		return q
	}
	return strings.TrimSpace(q[i+len("EXPLAIN"):])
}

// buildExplain 分析 SELECT 形态产出计划行。
func (h *kidbHandler) buildExplain(ctx context.Context, inner string) ([][2]string, error) {
	stmts, _, err := parser.New().Parse(inner, "", "")
	if err != nil {
		return nil, fmt.Errorf("%w: EXPLAIN 解析失败: %v", kidb.ErrUnsupported, err)
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("%w: EXPLAIN 只接受单条语句", kidb.ErrUnsupported)
	}
	sel, ok := stmts[0].(*ast.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("%w: EXPLAIN 仅支持 SELECT", kidb.ErrUnsupported)
	}

	var rows [][2]string
	add := func(item, detail string) { rows = append(rows, [2]string{item, detail}) }

	// 快速路径形状优先（与实际执行同一识别序）
	if fp := matchFastPath(inner); fp != nil {
		def, _ := h.s.deps.Cache.Get(ctx, fp.table)
		if def != nil {
			switch fp.kind {
			case fpCountStar:
				add("plan", "fastpath:count_star")
				add("table", fp.table)
				add("detail", "Σ ZCOUNT(exp, (now, +inf)) over 16384 slots × exp_shards")
				add("fanout", "16384")
				return rows, nil
			case fpMin, fpMax:
				if idx := findRangeIndex(def, fp.column); idx != nil {
					name := "min"
					if fp.kind == fpMax {
						name = "max"
					}
					add("plan", "fastpath:"+name)
					add("table", fp.table)
					add("index", idx.ID)
					add("detail", "bucket endpoint merge (ZRANGE/ZREVRANGE 0 0) + 回表校验跳脏")
					return rows, nil
				}
			}
		}
	}

	table := tableOf(sel)
	if table == "" {
		return nil, fmt.Errorf("%w: EXPLAIN 不支持的 FROM 形态", kidb.ErrUnsupported)
	}
	def, err := h.s.deps.Cache.Get(ctx, table)
	if err != nil {
		return nil, err
	}
	if def == nil {
		return nil, fmt.Errorf("%w: 表 %q 不存在", kidb.ErrUnsupported, table)
	}
	add("table", table)

	// JOIN 分档展示
	if sel.From != nil && sel.From.TableRefs != nil && sel.From.TableRefs.Right != nil {
		add("plan", "join")
		add("detail", "档 1 主键查找/档 2 维表广播放行；其余报 ERR_UNSUPPORTED_JOIN（docs/04 §4.4）")
		return rows, nil
	}

	if sel.Where == nil {
		if orderByHasRangeIndex(sel, def) {
			col := orderByCol(sel)
			add("plan", "range_lookup(ordered)")
			add("index", rangeIndexID(def, col))
			add("fanout", "16384 buckets (seed) + top-k merge early stop on LIMIT")
			return rows, nil
		}
		add("plan", "fullscan")
		add("fanout", "16384 × exp_shards")
		add("guard", fullscanVerdict(ctx, h, def, inner))
		return rows, nil
	}

	// 谓词分析：点查 > 等值 > 范围 > 前缀 > 兜底
	if where := sel.Where; where != nil {
		if pks := pointPKs(where, def); len(pks) > 0 {
			add("plan", "point_get")
			add("pks", fmt.Sprint(len(pks)))
			add("fanout", fmt.Sprintf("%d rowkey HGETALL（pipeline 聚合）", len(pks)))
			return rows, nil
		}
		if col, vals := eqPredOn(def, where); col != "" {
			add("plan", "eq_lookup")
			idxID := eqIndexID(def, col)
			add("index", idxID)
			add("values", fmt.Sprint(len(vals)))
			add("fanout", fmt.Sprintf("16384 × %d values", len(vals)))
			add("l1_l2", "nearcache+singleflight（热值指纹）")
			h.addCardinality(ctx, add, def.Name, idxID)
			return rows, nil
		}
		if col := rangePredOn(def, where); col != "" {
			add("plan", "range_lookup(ordered)")
			idxID := rangeIndexID(def, col)
			add("index", idxID)
			add("fanout", "16384 buckets (seed) + k-way merge")
			add("note", "全局 score 有序流；LIMIT 早停由引擎停止消费达成")
			h.addCardinality(ctx, add, def.Name, idxID)
			return rows, nil
		}
		if col := prefixPredOn(def, where); col != "" {
			add("plan", "prefix_lookup(lex)")
			add("index", prefixIndexID(def, col))
			add("fanout", "16384 lex buckets (seed) + k-way merge")
			add("note", "ZRANGEBYLEX [p [p+\\xff；回表 HasPrefix 重判")
			return rows, nil
		}
	}

	add("plan", "fullscan")
	add("guard", fullscanVerdict(ctx, h, def, inner))
	return rows, nil
}

// ==== 形态分析助手（TiDB AST 保守提取；判不准返回空走兜底）====

func orderByCol(sel *ast.SelectStmt) string {
	if sel.OrderBy == nil || len(sel.OrderBy.Items) == 0 {
		return ""
	}
	if ce, ok := sel.OrderBy.Items[0].Expr.(*ast.ColumnNameExpr); ok {
		return ce.Name.Name.O
	}
	return ""
}

func rangeIndexID(def *meta.TableDef, col string) string {
	for _, idx := range def.Indexes {
		if idx.Kind == meta.IndexRange && !idx.Building && len(idx.Columns) == 1 && strings.EqualFold(idx.Columns[0], col) {
			return idx.ID
		}
	}
	return ""
}

func eqIndexID(def *meta.TableDef, col string) string {
	for _, idx := range def.Indexes {
		if idx.Kind != meta.IndexRange && !idx.Building && len(idx.Columns) == 1 && strings.EqualFold(idx.Columns[0], col) {
			return idx.ID
		}
	}
	return ""
}

func prefixIndexID(def *meta.TableDef, col string) string {
	for _, idx := range def.Indexes {
		if idx.PrefixCopy && !idx.Building && len(idx.Columns) == 1 && strings.EqualFold(idx.Columns[0], col) {
			return idx.ID
		}
	}
	return ""
}

// pointPKs 提取主键点集（pk = v / pk IN (...)）。
func pointPKs(where ast.ExprNode, def *meta.TableDef) []string {
	switch x := where.(type) {
	case *ast.BinaryOperationExpr:
		if x.Op == opcode.EQ {
			var colName string
			var ve ast.ValueExpr
			if ce, ok := x.L.(*ast.ColumnNameExpr); ok {
				colName = ce.Name.Name.O
				ve, _ = x.R.(ast.ValueExpr)
			} else if ce, ok := x.R.(*ast.ColumnNameExpr); ok {
				colName = ce.Name.Name.O
				ve, _ = x.L.(ast.ValueExpr)
			}
			if colName != "" && strings.EqualFold(colName, def.PK) && ve != nil {
				return []string{fmt.Sprint(ve.GetValue())}
			}
		}
	case *ast.PatternInExpr:
		if ce, ok := x.Expr.(*ast.ColumnNameExpr); ok && strings.EqualFold(ce.Name.Name.O, def.PK) {
			var out []string
			for _, item := range x.List {
				if ve, ok := item.(ast.ValueExpr); ok {
					out = append(out, fmt.Sprint(ve.GetValue()))
				}
			}
			return out
		}
	}
	return nil
}

// eqPredOn AND 树中首个有等值/唯一索引的列（返回值个数）。
func eqPredOn(def *meta.TableDef, where ast.ExprNode) (string, []string) {
	var found string
	var vals []string
	var walk func(e ast.ExprNode)
	walk = func(e ast.ExprNode) {
		be, ok := e.(*ast.BinaryOperationExpr)
		if !ok {
			return
		}
		if be.Op == opcode.LogicAnd {
			walk(be.L)
			walk(be.R)
			return
		}
		if be.Op != opcode.EQ {
			return
		}
		var colName string
		var ve ast.ValueExpr
		if ce, ok := be.L.(*ast.ColumnNameExpr); ok {
			colName, ve = ce.Name.Name.O, valueExprOf(be.R)
		} else if ce, ok := be.R.(*ast.ColumnNameExpr); ok {
			colName, ve = ce.Name.Name.O, valueExprOf(be.L)
		}
		if colName == "" || ve == nil || strings.EqualFold(colName, def.PK) {
			return
		}
		if found == "" && eqIndexID(def, colName) != "" {
			found = colName
			vals = []string{fmt.Sprint(ve.GetValue())}
		}
	}
	walk(where)
	return found, vals
}

func valueExprOf(e ast.ExprNode) ast.ValueExpr {
	ve, _ := e.(ast.ValueExpr)
	return ve
}

// rangePredOn AND 树中首个有范围索引的比较列。
func rangePredOn(def *meta.TableDef, where ast.ExprNode) string {
	cols := map[string]bool{}
	collectPredCols(where, cols)
	for c := range cols {
		if rangeIndexID(def, c) != "" {
			return c
		}
	}
	return ""
}

// prefixPredOn AND 树中首个常量前缀 LIKE 的 prefix_copy 列。
func prefixPredOn(def *meta.TableDef, where ast.ExprNode) string {
	cols := map[string]bool{}
	collectPrefixLikeCols(where, cols)
	for c := range cols {
		if prefixIndexID(def, c) != "" {
			return c
		}
	}
	return ""
}

// fullscanVerdict 全扫的守卫判定（EXPLAIN 展示用）。
func fullscanVerdict(ctx context.Context, h *kidbHandler, def *meta.TableDef, raw string) string {
	if err := h.allowFullscan(ctx, def, raw); err != nil {
		return "ERR_NO_INDEX（hint /*+ FULLSCAN */ 或表白名单可放行）"
	}
	return "允许（hint/白名单）→ exp 登记册遍历 + 回表校验"
}

// addCardinality 附加索引基数估算行（HLL 采样，近似——docs/04 §4.6 决策依据）。
func (h *kidbHandler) addCardinality(ctx context.Context, add func(string, string), table, idxID string) {
	if idxID == "" {
		return
	}
	n, err := h.s.deps.Exec.IndexCardinality(ctx, table, idxID)
	if err != nil {
		return
	}
	add("cardinality(approx)", fmt.Sprint(n))
}
