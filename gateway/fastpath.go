package gateway

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"

	"kidb/meta"
)

// fastpath.go：KiDB 侧物理下推的网关快速路径（docs/04 §4.1/§4.5）。
//
// 识别（TiDB parser AST 形状判定，白名单制——判不准一律回退引擎路径）：
//   - SELECT COUNT(*) FROM t                        → exp 登记册 ZCOUNT 汇总（任意时刻精确）
//   - SELECT MIN(col)/MAX(col) FROM t（col 有范围索引）→ 桶端点归并 + 回表校验
//
// 为什么不用 gms 分析器规则：v0.20 无公开规则注入面；网关层形状识别的收益相同且
// 正确性只依赖我们自己验证过的执行器。这是 TiDB parser 在 DML 侧的**识别性**使用
// （执行语义仍归 gms——docs/02 §2.3 纪律的 v5.1 注记）。

// fpKind 快速路径类型。
type fpKind int

const (
	fpNone fpKind = iota
	fpCountStar
	fpMin
	fpMax
)

// fastPath 识别结果。
type fastPath struct {
	kind    fpKind
	table   string
	column  string // MIN/MAX 列
	outName string // 输出列名（含别名）
}

// matchFastPath 识别白名单内的 SELECT 形状；其余返回 nil。
func matchFastPath(query string) *fastPath {
	stmts, _, err := parser.New().Parse(query, "", "")
	if err != nil || len(stmts) != 1 {
		return nil
	}
	return matchFastPathAST(stmts[0])
}

// matchFastPathAST 在已解析 AST 上识别（analyzeDML 单 parse 联合评估的另一半）。
func matchFastPathAST(stmt ast.StmtNode) *fastPath {
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok {
		return nil
	}
	// 单表、无 WHERE/GROUP BY/HAVING/LIMIT/ORDER BY/DISTINCT/LOCK
	if sel.Where != nil || sel.GroupBy != nil || sel.Having != nil || sel.Limit != nil ||
		sel.OrderBy != nil || sel.Distinct || sel.LockInfo != nil {
		return nil
	}
	if sel.From == nil || sel.From.TableRefs == nil || sel.From.TableRefs.Right != nil {
		return nil // JOIN 形态（右枝非空）
	}
	ts, ok := sel.From.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok || ts.AsName.O != "" {
		return nil
	}
	if sel.Fields == nil || len(sel.Fields.Fields) != 1 {
		return nil
	}
	f := sel.Fields.Fields[0]
	agg, ok := f.Expr.(*ast.AggregateFuncExpr)
	if !ok {
		return nil
	}
	name := strings.ToLower(agg.F)
	fp := &fastPath{table: tn.Name.O}
	fp.outName = fp.computeOutName(f, name)

	switch {
	case name == "count" && len(agg.Args) == 1:
		// TiDB parser 把 COUNT(*) 归一为字面量 1（test_driver.ValueExpr）
		if ve, ok := agg.Args[0].(ast.ValueExpr); ok && fmt.Sprint(ve.GetValue()) == "1" {
			fp.kind = fpCountStar
			return fp
		}
	case (name == "min" || name == "max") && len(agg.Args) == 1:
		col, ok := agg.Args[0].(*ast.ColumnNameExpr)
		if !ok {
			return nil
		}
		fp.column = col.Name.Name.O
		if name == "min" {
			fp.kind = fpMin
		} else {
			fp.kind = fpMax
		}
		return fp
	}
	return nil
}

// computeOutName 输出列名（别名优先，否则 MySQL 惯例原文）。
func (fp *fastPath) computeOutName(f *ast.SelectField, fname string) string {
	if f.AsName.O != "" {
		return f.AsName.O
	}
	switch fname {
	case "count":
		return "COUNT(*)"
	case "min":
		return "MIN(" + fp.column + ")"
	case "max":
		return "MAX(" + fp.column + ")"
	}
	return fname
}

// tryFastPath 命中则执行并返回结果；未命中/不满足前提返回 (nil, false, nil) 回退引擎。
func (h *kidbHandler) tryFastPath(ctx context.Context, fp *fastPath) (*sqltypes.Result, bool, error) {
	def, err := h.s.deps.Cache.Get(ctx, fp.table)
	if err != nil || def == nil {
		return nil, false, err
	}
	switch fp.kind {
	case fpCountStar:
		n, err := h.s.deps.Exec.RowCount(ctx, def, time.Now().Unix())
		if err != nil {
			return nil, false, err
		}
		return intResult(fp.outName, n), true, nil
	case fpMin, fpMax:
		idx := findRangeIndex(def, fp.column)
		if idx == nil {
			return nil, false, nil // 无范围索引 → 回退引擎（全扫聚合，正确）
		}
		score, _, found, err := h.s.deps.Exec.MinMax(ctx, def, idx, fp.kind == fpMin)
		if err != nil {
			return nil, false, err
		}
		if !found {
			// 空集 → 单行 NULL（MySQL 聚合语义）
			return &sqltypes.Result{
				Fields: []*querypb.Field{{Name: fp.outName, Type: querypb.Type_NULL_TYPE}},
				Rows:   [][]sqltypes.Value{{sqltypes.MakeTrusted(querypb.Type_NULL_TYPE, nil)}},
			}, true, nil
		}
		return fp.scoreResult(def, score)
	}
	return nil, false, nil
}

// findRangeIndex 找列上的范围索引。
func findRangeIndex(def *meta.TableDef, col string) *meta.IndexDef {
	for i := range def.Indexes {
		idx := &def.Indexes[i]
		if idx.Kind == meta.IndexRange && !idx.Building && len(idx.Columns) == 1 && strings.EqualFold(idx.Columns[0], col) {
			return idx
		}
	}
	return nil
}

// scoreResult 按列类型构造 MIN/MAX 结果。
func (fp *fastPath) scoreResult(def *meta.TableDef, score float64) (*sqltypes.Result, bool, error) {
	col, ok := def.Column(fp.column)
	if !ok {
		return nil, false, nil
	}
	var vt querypb.Type
	var lit string
	switch col.Type {
	case meta.ColInt:
		vt = querypb.Type_INT64
		lit = strconv.FormatInt(int64(score), 10)
	case meta.ColFloat:
		vt = querypb.Type_FLOAT64
		lit = strconv.FormatFloat(score, 'g', -1, 64)
	case meta.ColTimestamp:
		vt = querypb.Type_INT64 // Unix 秒（v1：时间戳列以秒值返回）
		lit = strconv.FormatInt(int64(score), 10)
	default:
		return nil, false, nil // 非常规类型回退引擎
	}
	return &sqltypes.Result{
		Fields: []*querypb.Field{{Name: fp.outName, Type: vt}},
		Rows:   [][]sqltypes.Value{{sqltypes.MakeTrusted(vt, []byte(lit))}},
	}, true, nil
}

// intResult 构造单行整数结果。
func intResult(name string, n uint64) *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{{Name: name, Type: querypb.Type_UINT64}},
		Rows:   [][]sqltypes.Value{{sqltypes.MakeTrusted(querypb.Type_UINT64, []byte(strconv.FormatUint(n, 10)))}},
	}
}
