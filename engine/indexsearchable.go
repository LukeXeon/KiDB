package engine

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"kidb/meta"
)

// indexsearchable.go：sql.IndexSearchableTable 实现（docs/04 §4.5 前缀搜索的
// 引擎接入口）。
//
// 只认领一种形态：**常量前缀 LIKE 落在 prefix_copy 索引列上**
// （`WHERE city LIKE 'abc%'` → 字典序副本 ZRANGEBYLEX 路径）。
// 其余谓词一律返回 ok=false 交回 gms coster（既有 range 翻译通道）。
//
// 为什么走这个接口而不是 FilteredTable：gms v0.20 的 FilteredTable 只在
// bindvar 规则内被调用（非常驻分析面）；IndexSearchableTable 是
// costedIndexScans 的正式接入口（analyze 每次必过）。

// LookupForExpressions 实现 sql.IndexSearchableTable。
func (t *Table) LookupForExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.IndexLookup, *sql.FuncDepSet, sql.Expression, bool, error) {
	for i, e := range exprs {
		like, ok := e.(*expression.Like)
		if !ok || like.Escape != nil {
			continue
		}
		gf, ok := like.Left().(*expression.GetField)
		if !ok {
			continue
		}
		lit, ok := like.Right().(*expression.Literal)
		if !ok {
			continue
		}
		pat, ok := lit.Value().(string)
		if !ok {
			continue
		}
		prefix, ok := constPrefixOf(pat)
		if !ok {
			continue
		}
		idx := t.prefixIndexOn(gf.Name())
		if idx == nil {
			continue
		}
		// 构造前缀区间 lookup：[prefix, prefix+\xff)（ZRANGEBYLEX 界的源）
		engIdx := &Index{id: idx.ID, table: t.def.Name, cols: idx.Columns,
			unique: idx.Kind == meta.IndexUnique, deps: t.deps, def: t.def}
		colExpr := t.def.Name + "." + idx.Columns[0]
		b := sql.NewMySQLIndexBuilder(engIdx)
		b.GreaterOrEqual(ctx, colExpr, prefix)
		b.LessThan(ctx, colExpr, prefix+"\xff")
		ranges := b.Ranges(ctx)
		if len(ranges) == 0 {
			return sql.IndexLookup{}, nil, nil, false, nil
		}
		lookup := sql.NewIndexLookup(engIdx, ranges, false, false, false, false)

		// 残余谓词（其余合取项留给 gms Filter；LIKE 本身由回表校验
		// HasPrefix 重判——preciseIndexAccess=true 时 gms 会丢弃它）
		var rest []sql.Expression
		rest = append(rest, exprs[:i]...)
		rest = append(rest, exprs[i+1:]...)
		var residual sql.Expression
		if len(rest) > 0 {
			residual = expression.JoinAnd(rest...)
		}
		return lookup, nil, residual, true, nil
	}
	return sql.IndexLookup{}, nil, nil, false, nil
}

// SkipIndexCosting false：未认领形态回退 gms coster（既有行为；
// coster 安全性由 PrimaryKeySchema 恒全量保证，projection_test 钉死）。
func (t *Table) SkipIndexCosting() bool { return false }

// constPrefixOf 提取常量前缀 LIKE 的前缀：恰一个 % 结尾、无 _/转义、前缀非空。
// 其余 LIKE 形态（中缀/后缀/多通配）返回 false——走无索引纪律或引擎过滤。
func constPrefixOf(pat string) (string, bool) {
	if !strings.HasSuffix(pat, "%") || len(pat) < 2 {
		return "", false
	}
	prefix := pat[:len(pat)-1]
	if strings.ContainsAny(prefix, "%_\\") {
		return "", false
	}
	return prefix, true
}

// prefixIndexOn 找列上带字典序副本的索引（回填中不可见，docs/06 §6.3）。
func (t *Table) prefixIndexOn(col string) *meta.IndexDef {
	for i := range t.def.Indexes {
		idx := &t.def.Indexes[i]
		if !idx.PrefixCopy || idx.Building || len(idx.Columns) != 1 {
			continue
		}
		if strings.EqualFold(idx.Columns[0], col) {
			c, ok := t.def.Column(col)
			if ok && c.Type == meta.ColString {
				return idx
			}
		}
	}
	return nil
}

// 编译期接口断言。
var _ sql.IndexSearchableTable = (*Table)(nil)
