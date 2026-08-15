package engine

import (
	"fmt"

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
		gt, err := columnTypeFromText(col.TypeText)
		if err != nil {
			continue // Catalog 写坏（唯一写入点 TableFromSchema）；GetIndexes 面防御性跳过
		}
		out = append(out, sql.ColumnExpressionType{Expression: i.table + "." + c, Type: gt})
	}
	return out
}

// CanSupport 范围过滤支持（gms 索引选路咨询）。
//
// 收紧纪律（gms replace_sort.go 契约的防御面）：gms 在 ORDER BY 列与索引前缀
// 匹配时直接删除 Sort 节点（ASC 不咨询任何接口）——无序路径一旦被选为排序载体
// 就会静默产出乱序。因此只有 score 有序流（topk.go k 路归并）的范围索引
// 接受任意区间；等值/唯一/主键只接受点范围（点集内序由 translate 排序保证），
// 非点范围退回全扫 + 引擎层 sort（正确性优先，代价与既有 fullScanFallback 相同）。
// 唯一例外：带 prefix_copy 的等值/唯一索引额外接受**前缀区间形态**
// [p, p+\xff)（该形态只由 LookupForExpressions 自产——字典序副本路径
// 产出全局字典序，同样满足有序契约）。
func (i *Index) CanSupport(ctx *sql.Context, ranges ...sql.Range) bool {
	if !i.primary {
		if idx := i.def.Index(i.id); idx != nil {
			if idx.Kind == meta.IndexRange {
				return true
			}
			if idx.PrefixCopy {
				for _, r := range ranges {
					if isPointRange(r) {
						continue
					}
					if _, _, ok := prefixRangeBounds(r); !ok {
						return false
					}
				}
				return true
			}
		}
	}
	for _, r := range ranges {
		if !isPointRange(r) {
			return false
		}
	}
	return true
}

// isPointRange 判定单点范围（闭下界 == 闭上界）。
func isPointRange(r sql.Range) bool {
	mr, ok := r.(sql.MySQLRange)
	if !ok || len(mr) != 1 {
		return false
	}
	lo, lok := belowKey(mr[0].LowerBound)
	hi, hok := aboveKey(mr[0].UpperBound)
	if !lok || !hok {
		return false
	}
	return fmt.Sprint(lo) == fmt.Sprint(hi)
}

// prefixRangeBounds 提取前缀区间形态的界值：单列 [lo, hi) 且 hi == lo+"\xff"
// （即 LookupForExpressions 经 GreaterOrEqual(p)+LessThan(p+\xff) 构造的形状）。
func prefixRangeBounds(r sql.Range) (string, string, bool) {
	mr, ok := r.(sql.MySQLRange)
	if !ok || len(mr) != 1 {
		return "", "", false
	}
	lo, ok := mr[0].LowerBound.(sql.Below) // 闭下界 [lo
	if !ok {
		return "", "", false
	}
	hi, ok := mr[0].UpperBound.(sql.Below) // 开上界 hi)（gms 语义：Below 作上界为开）
	if !ok {
		return "", "", false
	}
	loS, ok1 := lo.Key.(string)
	hiS, ok2 := hi.Key.(string)
	if !ok1 || !ok2 || loS == "" {
		return "", "", false
	}
	return loS, hiS, hiS == loS+"\xff"
}

// Order 索引扫描产出序（sql.OrderedIndex，gms replace_sort.go Case B 的裁决面）：
// 范围索引 = Asc（exec topk.go 全局 score 有序流）；其余 = None
// （None 阻止 gms 把全表扫描改写成无序索引访问后删 Sort——
// 等值桶/主键无有序结构，删 Sort 即静默乱序）。
func (i *Index) Order() sql.IndexOrder {
	if !i.primary {
		if idx := i.def.Index(i.id); idx != nil && idx.Kind == meta.IndexRange {
			return sql.IndexOrderAsc
		}
	}
	return sql.IndexOrderNone
}

// Reversible 范围索引可反向迭代（ZREVRANGEBYSCORE）；其余不可
// （gms Case A DESC 对不可逆索引保留 Sort——正确性由引擎层 sort 承载）。
func (i *Index) Reversible() bool {
	return i.Order() == sql.IndexOrderAsc
}

// CanSupportOrderBy 范围索引支持 ORDER BY（score 有序）；
// 等值/主键不支持（nearest-neighbor 类查询的 gms 咨询面）。
func (i *Index) CanSupportOrderBy(expr sql.Expression) bool {
	return i.Order() == sql.IndexOrderAsc
}

// PrefixLengths 无前缀长度（docs/02 §2.4：前缀长度索引不支持，走 prefix_copy）。
func (i *Index) PrefixLengths() []uint16 { return nil }

var (
	_ sql.Index        = (*Index)(nil)
	_ sql.OrderedIndex = (*Index)(nil)
)
