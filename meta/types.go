// Package meta 定义 Catalog/BucketMap 的内存模型与读写存储
// （docs/06：元数据即数据，存于 Redis 自身，版本校验 + lease 传播）。
package meta

import (
	"fmt"
	"strings"
)

// ColumnType 是 DDL 列类型白名单（docs/02 §2.4）。
type ColumnType int

const (
	ColInt       ColumnType = iota + 1 // BIGINT/INT/TINYINT/BOOLEAN
	ColFloat                           // DOUBLE/FLOAT/DECIMAL
	ColString                          // VARCHAR/CHAR
	ColBytes                           // VARBINARY/BLOB
	ColTimestamp                       // DATETIME/TIMESTAMP（存 Unix 秒）
	ColJSON                            // JSON（嵌套 blob）
)

func (t ColumnType) String() string {
	switch t {
	case ColInt:
		return "INT"
	case ColFloat:
		return "FLOAT"
	case ColString:
		return "STRING"
	case ColBytes:
		return "BYTES"
	case ColTimestamp:
		return "TIMESTAMP"
	case ColJSON:
		return "JSON"
	}
	return "UNKNOWN"
}

// RangeIndexable 报告该列类型是否允许建范围索引
// （docs/03 §3.4：数值/时间戳可；string 走等值/字典序）。
func (t ColumnType) RangeIndexable() bool {
	return t == ColInt || t == ColFloat || t == ColTimestamp
}

// ColumnDef 是列定义。
type ColumnDef struct {
	Name    string     `json:"name"`
	Type    ColumnType `json:"type"`
	NotNull bool       `json:"not_null"`
}

// IndexKind 是索引形态（docs/03 §3.3：桶模型按形态分桶规则）。
type IndexKind int

const (
	IndexEq     IndexKind = iota + 1 // 等值索引（每值每 slot 一桶）
	IndexRange                       // 范围索引（score=字段值）
	IndexUnique                      // 唯一索引（等值桶 + 预约 key 判定，docs/05 §5.3）
)

func (k IndexKind) String() string {
	switch k {
	case IndexEq:
		return "EQ"
	case IndexRange:
		return "RANGE"
	case IndexUnique:
		return "UNIQUE"
	}
	return "UNKNOWN"
}

// IndexDef 是索引定义。v5.0 索引限单列（覆盖列经 Covering 声明）。
type IndexDef struct {
	ID         string    `json:"id"`
	Columns    []string  `json:"columns"` // 长度恒为 1（DDL 校验）
	Kind       IndexKind `json:"kind"`
	Covering   []string  `json:"covering,omitempty"`    // 覆盖列（docs/03 §3.5）
	Async      bool      `json:"async,omitempty"`       // 异步索引（docs/05 §5.2；与 Unique 互斥）
	PrefixCopy bool      `json:"prefix_copy,omitempty"` // 字典序副本（前缀搜索）
}

// TableDef 是表定义（Catalog 的核心载荷）。
type TableDef struct {
	Name         string      `json:"name"`
	Columns      []ColumnDef `json:"columns"`
	PK           string      `json:"pk"` // 单列主键（DDL 校验）
	Indexes      []IndexDef  `json:"indexes,omitempty"`
	DefaultTTL   int64       `json:"default_ttl,omitempty"`   // 秒；0=无默认
	MaxRowBytes  int         `json:"max_row_bytes,omitempty"` // 默认 1MB，硬上限 4MB
	ExpectedRows string      `json:"expected_rows,omitempty"`
	ExpShards    int         `json:"exp_shards,omitempty"` // exp 登记册细分（docs/07 §7.2），默认 1
	Dimension    bool        `json:"dimension,omitempty"`  // 维表标记（docs/04 §4.4 档 2）
	Ver          uint64      `json:"-"`                    // 表级 _ver（来自 Catalog，不随 def 编码）
}

// Column 按名取列。
func (t *TableDef) Column(name string) (ColumnDef, bool) {
	for _, c := range t.Columns {
		if strings.EqualFold(c.Name, name) {
			return c, true
		}
	}
	return ColumnDef{}, false
}

// Index 按 ID 取索引。
func (t *TableDef) Index(id string) *IndexDef {
	for i := range t.Indexes {
		if strings.EqualFold(t.Indexes[i].ID, id) {
			return &t.Indexes[i]
		}
	}
	return nil
}

// EffectiveExpShards 返回登记册细分片数（默认 1）。
func (t *TableDef) EffectiveExpShards() int {
	if t.ExpShards <= 0 {
		return 1
	}
	return t.ExpShards
}

// EffectiveMaxRowBytes 返回行体积上限（默认 1MB）。
func (t *TableDef) EffectiveMaxRowBytes() int {
	if t.MaxRowBytes <= 0 {
		return 1 << 20
	}
	return t.MaxRowBytes
}

// ValidateReserved 拒绝保留列命名（docs/07 §7.1：`_` 前缀是引擎命名空间）。
func ValidateReserved(name string) error {
	if strings.HasPrefix(name, "_") {
		return fmt.Errorf("column/index %q uses reserved '_' prefix", name)
	}
	return nil
}
