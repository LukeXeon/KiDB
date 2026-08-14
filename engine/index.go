package engine

import (
	"github.com/dolthub/go-mysql-server/sql"

	"kidb/meta"
)

// Index 实现 sql.Index（KiDB 二级索引 + 隐式 PRIMARY 的引擎侧表达）。
type Index struct {
	id      string
	table   string
	cols    []string
	unique  bool
	primary bool
	deps    Deps
	def     *meta.TableDef
}

// ID 索引标识。
func (i *Index) ID() string { return i.id }

// Database 所属库（扁平命名空间）。
func (i *Index) Database() string { return "kidb" }

// Table 所属表。
func (i *Index) Table() string { return i.table }

// Expressions 索引列表达式（gms 期望表限定形式 "table.col"——
// 分析器用 GetField.String() 的全限定名匹配索引表达式）。
func (i *Index) Expressions() []string {
	out := make([]string, 0, len(i.cols))
	for _, c := range i.cols {
		out = append(out, i.table+"."+c)
	}
	return out
}

// IsUnique 是否唯一。
func (i *Index) IsUnique() bool { return i.unique }

// IsSpatial 否。
func (i *Index) IsSpatial() bool { return false }

// IsFullText 否。
func (i *Index) IsFullText() bool { return false }

// IsVector 否。
func (i *Index) IsVector() bool { return false }

// Comment 索引注释。
func (i *Index) Comment() string { return "" }

// IndexType 索引类型（KiDB 桶模型对外呈 BTREE 语义）。
func (i *Index) IndexType() string { return "BTREE" }

// IsGenerated 否。
func (i *Index) IsGenerated() bool { return false }

// ColumnExpressionTypes 列表达式类型（Expression 用表限定形式 "table.col"——
// gms 按 GetField 全限定名匹配，index_builder.go NewMySQLIndexBuilder 即以此为键）。
func (i *Index) ColumnExpressionTypes() []sql.ColumnExpressionType {
	out := make([]sql.ColumnExpressionType, 0, len(i.cols))
	for _, c := range i.cols {
		col, ok := i.def.Column(c)
		if !ok {
			continue
		}
		gt, err := goType(col.Type)
		if err != nil {
			continue
		}
		out = append(out, sql.ColumnExpressionType{Expression: i.table + "." + c, Type: gt})
	}
	return out
}

// CanSupport 范围过滤支持（单列索引：等值/范围均可）。
func (i *Index) CanSupport(ctx *sql.Context, ranges ...sql.Range) bool { return true }

// CanSupportOrderBy 范围索引支持 ORDER BY（score 有序）；
// 等值/主键不支持（v1：排序由引擎层完成，docs/04 §4.5 端点归并随后续批次接入）。
func (i *Index) CanSupportOrderBy(expr sql.Expression) bool {
	if i.primary {
		return false
	}
	idx := i.def.Index(i.id)
	if idx == nil {
		return false
	}
	return idx.Kind == meta.IndexRange
}

// PrefixLengths 无前缀长度（docs/02 §2.4：前缀长度索引不支持，走 prefix_copy）。
func (i *Index) PrefixLengths() []uint16 { return nil }

var _ sql.Index = (*Index)(nil)
