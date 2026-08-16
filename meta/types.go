// Package meta 定义 Catalog/BucketMap 的内存模型与读写存储
// （docs/06：元数据即数据，存于 Redis 自身，版本校验 + lease 传播）。
package meta

import (
	"fmt"
	"strings"

	"kidb/i18n"
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
//
// 忠实类型记录（docs/02 §2.2 用户裁决）：Type 是存储类（编码/索引形态的依据），
// TypeText 是 gms 解析产物的规范类型文本（Type.String() 原样，如 "varchar(32)"）——
// schema 输出按 TypeText 忠实重建 gms 类型，varchar 长度全程不丢。两字段解耦：
// 文本演进不影响存量数据的存储编码。DDL 入口（engine/ddlconvert.go）是唯一写入点。
type ColumnDef struct {
	Name     string     `json:"name"`
	Type     ColumnType `json:"type"`
	TypeText string     `json:"type_text"` // gms 规范类型文本（"varchar(32)"/"bigint"/"datetime"）
	NotNull  bool       `json:"not_null"`
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
	Building   bool      `json:"building,omitempty"`    // 在线回填中（docs/06 §6.3：完成前不对查询可见）
}

// DDLJob 是进行中的 DDL 作业（Catalog `_job` 字段载荷，docs/06 §6.3）。
// 状态持久化 → 任意网关实例的 Controller 巡检接管续作。
type DDLJob struct {
	Type    string    `json:"type"` // "create_index"
	Index   *IndexDef `json:"index,omitempty"`
	Cursor  int       `json:"cursor"` // 回填进度游标（slot 号，0..16384）
	Started int64     `json:"started"`
}

// DropJob 是进行中的 DROP TABLE 清理作业（docs/06 §6.3；登记于 `c:dropjobs`
// Hash——Catalog 已在 DROP 时删除，作业须自带 def 快照）。
type DropJob struct {
	Table   string    `json:"table"`
	Cursor  int       `json:"cursor"` // slot 游标（0..16384）
	Started int64     `json:"started"`
	Def     *TableDef `json:"def"` // 删除时的表定义快照（清理索引用）
}

// TableDef 是表定义（Catalog 的核心载荷）。
type TableDef struct {
	Name           string      `json:"name"`
	Columns        []ColumnDef `json:"columns"`
	PK             string      `json:"pk"` // 单列主键（DDL 校验）
	Indexes        []IndexDef  `json:"indexes,omitempty"`
	DefaultTTL     int64       `json:"default_ttl,omitempty"` // 秒；0=无默认（payload 唯一表级语义字段）
	AutoIncrColumn string      `json:"auto_incr,omitempty"`   // AUTO_INCREMENT 列（限主键、INT，DDL 校验）
	Ver            uint64      `json:"-"`                     // 表级 _ver（来自 Catalog，不随 def 编码）
	// 设计原点纪律（docs/01 §1.0）：以下曾是 payload 字段，现已转自动/内置——
	// max_row_bytes 固定 1MB（tuning.toml txguard.max_row_bytes）；
	// exp 登记册细分恒 1（自动细分为自治后续项）；维表判定按实时行数；
	// 字典序副本对字符串等值/唯一索引自动开启。
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

// EffectiveExpShards 返回登记册细分片数（恒 1——自动细分为自治后续项，
// 机制（ExpKeyN 分片键）保留在 keycodec，启用时无需格式演进）。
func (t *TableDef) EffectiveExpShards() int {
	return 1
}

// TTLPseudoColumn 行级 TTL 伪列（docs/07 §7.1）：写入 >0 秒设行 TTL /
// 0 或 NULL 承表级 default_ttl / <0 软删除；读出 = 剩余 TTL 秒（-1 无 TTL）。
// 引擎元数据列（`_` 前缀命名空间），用户 DDL 不可声明（ValidateReserved 拒绝）。
const TTLPseudoColumn = "_ttl"

// ValidateReserved 拒绝保留列命名（docs/07 §7.1：`_` 前缀是引擎命名空间）。
func ValidateReserved(name string) error {
	if strings.HasPrefix(name, "_") {
		return fmt.Errorf("%s", i18n.T("meta.reserved_prefix", name))
	}
	return nil
}
