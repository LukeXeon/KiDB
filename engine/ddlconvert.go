package engine

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"kidb"
	"kidb/i18n"
	"kidb/meta"
)

// ddlconvert.go：DDL 语义层（docs/02 §2.3）——把 gms 的 DDL 计划产物
// （PrimaryKeySchema/IndexDef/COMMENT 串）转换为 KiDB Catalog 定义并执行全部校验。
// **类型语义以 gms 为准**：Catalog 忠实记录 gms 解析产物的完整类型
// （存储类 + 规范类型文本），schema 重建见 typetext.go。

// kidbCommentPrefix 是 KiDB 扩展在 COMMENT 中的前缀。
// 非此前缀的 COMMENT 视为普通注释，两不干扰。
const kidbCommentPrefix = "kidb:"

// ==== COMMENT payload（docs/02 §2.3：只留语义声明，严格解析）====

// tableOpts 表级 payload：仅 default_ttl（行级 TTL 是缓存定位的核心语义）。
// 未知字段报错（"不支持直接报错"优于静默忽略，docs/01 §1.0）。
type tableOpts struct {
	DefaultTTL int64 `json:"default_ttl"`
}

// indexOpts 索引级 payload：仅 covering / async。
type indexOpts struct {
	Covering []string `json:"covering"` // 覆盖列（docs/03 §3.5；列须 NOT NULL）
	Async    bool     `json:"async"`    // 异步索引（docs/05 §5.2；与唯一索引互斥）
}

// ParseTableComment 解析表级 COMMENT（非 kidb: 前缀 = 无选项）。
func ParseTableComment(comment string) (int64, error) {
	v, ok := strings.CutPrefix(comment, kidbCommentPrefix)
	if !ok {
		return 0, nil
	}
	var to tableOpts
	dec := json.NewDecoder(strings.NewReader(v))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&to); err != nil {
		return 0, fmt.Errorf("%s: %w", i18n.T("ddl.bad_table_opts"), err)
	}
	if to.DefaultTTL < 0 {
		return 0, fmt.Errorf("%s", i18n.T("ddl.default_ttl_negative"))
	}
	return to.DefaultTTL, nil
}

// ParseIndexComment 解析索引级 COMMENT。
func ParseIndexComment(comment string) (indexOpts, error) {
	var io indexOpts
	v, ok := strings.CutPrefix(comment, kidbCommentPrefix)
	if !ok {
		return io, nil
	}
	dec := json.NewDecoder(strings.NewReader(v))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&io); err != nil {
		return io, fmt.Errorf("%s: %w", i18n.T("ddl.bad_index_opts"), err)
	}
	return io, nil
}

// ==== 类型映射：gms sql.Type → meta.ColumnType 存储类（docs/02 §2.3 白名单）====

// ColumnTypeOf 列存储类映射（类型语义以 gms 为准；本函数只决定编码/索引形态）。
// 白名单：整数/浮点/字符串/二进制/时间戳/JSON；DECIMAL/DATE/TIME/枚举等
// 明确报错（范围索引 score 语义与编码精度不担保的类型不放行）。
// DATETIME/TIMESTAMP 带 (p) 小数秒精度拒绝——存储形态是 Unix 秒，
// 放行会产生写入即静默截断（review 实证：datetime(6) 往返丢 .123456）。
func ColumnTypeOf(ct sql.Type) (meta.ColumnType, error) {
	switch {
	case types.IsInteger(ct):
		return meta.ColInt, nil
	case types.IsFloat(ct):
		return meta.ColFloat, nil
	case types.IsDecimal(ct):
		return 0, fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.decimal_unsupported"))
	case types.IsJSON(ct): // 先于二进制：gms IsBinaryType 把 TypeJSON 也算在内
		return meta.ColJSON, nil
	case types.IsBinaryType(ct):
		return meta.ColBytes, nil
	case types.IsText(ct):
		return meta.ColString, nil
	case types.IsDatetimeType(ct) || types.IsTimestampType(ct):
		if p, ok := ct.(interface{ Precision() int }); ok && p.Precision() > 0 {
			return 0, fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.datetime_precision", ct.String()))
		}
		return meta.ColTimestamp, nil
	}
	return 0, fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.column_type_unsupported", ct))
}

// ==== CREATE TABLE：gms PrimaryKeySchema → TableDef ====

// TableFromSchema 由 gms 主键 schema 与表 COMMENT 构造表定义（校验全规则）。
// 忠实类型记录：列定义存 存储类（ColumnTypeOf）+ 规范类型文本（c.Type.String()）。
func TableFromSchema(name string, sch sql.PrimaryKeySchema, comment string) (*meta.TableDef, error) {
	if err := meta.ValidateIdent(name); err != nil {
		return nil, fmt.Errorf("%w: %v", kidb.ErrUnsupported, err)
	}
	if len(sch.Schema) == 0 || len(sch.Schema) > 256 {
		return nil, fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.column_count_range", len(sch.Schema)))
	}
	if len(sch.PkOrdinals) != 1 {
		return nil, fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.single_pk_required", len(sch.PkOrdinals)))
	}
	ttl, err := ParseTableComment(comment)
	if err != nil {
		return nil, err
	}
	def := &meta.TableDef{Name: name, DefaultTTL: ttl}
	seen := map[string]bool{}
	for i, c := range sch.Schema {
		if err := meta.ValidateReserved(c.Name); err != nil {
			return nil, fmt.Errorf("%w: %v", kidb.ErrUnsupported, err)
		}
		if err := meta.ValidateIdent(c.Name); err != nil {
			return nil, fmt.Errorf("%w: %v", kidb.ErrUnsupported, err)
		}
		if seen[strings.ToLower(c.Name)] {
			return nil, fmt.Errorf("%s", i18n.T("ddl.column_duplicate", c.Name))
		}
		seen[strings.ToLower(c.Name)] = true
		ct, err := ColumnTypeOf(c.Type)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", i18n.T("ddl.column_label", c.Name), err)
		}
		// 列级属性白名单（docs/01 §1.0：不支持直接报错，优于静默丢弃）：
		// Catalog 只记录类型/可空性/自增，DEFAULT/ON UPDATE/列级 COLLATE
		// 若静默丢弃会产生与用户声明不符的行为——明确拒绝。
		if c.Default != nil {
			return nil, fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.default_unsupported", c.Name))
		}
		if c.OnUpdate != nil {
			return nil, fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.on_update_unsupported", c.Name))
		}
		if tc, ok := c.Type.(sql.TypeWithCollation); ok && tc.Collation() != sql.Collation_Default {
			return nil, fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.collation_unsupported", c.Name))
		}
		def.Columns = append(def.Columns, meta.ColumnDef{
			Name: c.Name, Type: ct, TypeText: c.Type.String(), NotNull: !c.Nullable,
		})
		if c.AutoIncrement {
			def.AutoIncrColumn = c.Name
		}
		if i == sch.PkOrdinals[0] {
			def.PK = c.Name
		}
	}
	return def, validateTable(def)
}

// validateTable 表级校验（docs/02 §2.3 + docs/06 §6.1）。
func validateTable(t *meta.TableDef) error {
	if t.PK == "" {
		return fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.pk_required"))
	}
	pkCol, ok := t.Column(t.PK)
	if !ok {
		return fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.pk_column_missing", t.PK))
	}
	if pkCol.Type != meta.ColInt && pkCol.Type != meta.ColString {
		return fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.pk_type_limit"))
	}
	if t.AutoIncrColumn != "" && (t.AutoIncrColumn != t.PK || pkCol.Type != meta.ColInt) {
		return fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.auto_incr_limit"))
	}
	if len(t.Indexes) > 16 {
		return fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.too_many_indexes", len(t.Indexes)))
	}
	seen := map[string]bool{}
	for i := range t.Indexes {
		idx := &t.Indexes[i]
		if err := meta.ValidateReserved(idx.ID); err != nil {
			return fmt.Errorf("%w: %v", kidb.ErrUnsupported, err)
		}
		if seen[strings.ToLower(idx.ID)] {
			return fmt.Errorf("%s", i18n.T("ddl.index_name_duplicate", idx.ID))
		}
		seen[strings.ToLower(idx.ID)] = true
		if err := ValidateIndexForTable(idx, t); err != nil {
			return err
		}
	}
	return nil
}

// ==== 索引：gms sql.IndexDef → meta.IndexDef ====

// IndexFromDef 由 gms 索引定义构造（内联索引/ALTER ADD INDEX/CREATE INDEX 统一入口；
// IndexDef.Comment 携带 kidb payload，Constraint 携带 UNIQUE）。
func IndexFromDef(idxDef sql.IndexDef) (*meta.IndexDef, error) {
	if len(idxDef.Columns) != 1 {
		return nil, fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.index_single_column", idxDef.Name, len(idxDef.Columns)))
	}
	idx := &meta.IndexDef{ID: idxDef.Name, Columns: []string{idxDef.Columns[0].Name}}
	if idxDef.Constraint == sql.IndexConstraint_Unique {
		idx.Kind = meta.IndexUnique
	}
	opts, err := ParseIndexComment(idxDef.Comment)
	if err != nil {
		return nil, err
	}
	idx.Covering = opts.Covering
	idx.Async = opts.Async
	return idx, validateIndexShape(idx)
}

// validateIndexShape 索引形态校验（不依赖表定义的部分）。
func validateIndexShape(idx *meta.IndexDef) error {
	if err := meta.ValidateIdent(idx.ID); err != nil {
		return fmt.Errorf("%w: %v", kidb.ErrUnsupported, err)
	}
	if len(idx.Columns) != 1 {
		return fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.index_single_column", idx.ID, len(idx.Columns)))
	}
	if idx.Async && idx.Kind == meta.IndexUnique {
		return fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.unique_sync_required", idx.ID))
	}
	if idx.Async && len(idx.Covering) > 0 {
		return fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.async_no_covering", idx.ID))
	}
	return nil
}

// ValidateIndexForTable 索引对表校验（类型相关 + 形态推导）。
func ValidateIndexForTable(idx *meta.IndexDef, t *meta.TableDef) error {
	if err := validateIndexShape(idx); err != nil {
		return err
	}
	col, ok := t.Column(idx.Columns[0])
	if !ok {
		return fmt.Errorf("%s", i18n.T("ddl.index_column_missing", idx.Columns[0]))
	}
	// 形态推导（docs/02 §2.3）：唯一=UNIQUE；数值/时间戳=RANGE；其余=等值。
	if idx.Kind != meta.IndexUnique {
		if col.Type.RangeIndexable() {
			idx.Kind = meta.IndexRange
		} else {
			idx.Kind = meta.IndexEq
		}
	}
	// 字典序副本自动开启（docs/01 §1.0：前缀搜索开箱即用，无需声明）。
	if idx.Kind != meta.IndexRange && col.Type == meta.ColString {
		idx.PrefixCopy = true
	}
	for _, cc := range idx.Covering {
		cdef, ok := t.Column(cc)
		if !ok {
			return fmt.Errorf("%s", i18n.T("ddl.covering_column_missing", cc))
		}
		if !cdef.NotNull {
			return fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.covering_not_null", cc))
		}
	}
	return nil
}
