// Package ddl 是 KiDB 的 DDL 执行器入口（docs/02 §2.4）：
// 用 TiDB pkg/parser 解析 DDL（独立 module，零修改零 fork，docs/13 §13.3），
// KiDB 扩展经 COMMENT 载体（`kidb:{json}`）解析，产出 meta.TableDef / IndexDef，
// 交给 Catalog 作业流执行（docs/06 §6.3）。
//
// 注意：test_driver 的匿名导入是 TiDB parser 模块的既有约定
// （解析 DEFAULT 字面量等值表达式需要注册值驱动；它在 parser module 内部，
// 不构成额外耦合）。
package ddl

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"

	"kidb"
	"kidb/meta"
)

// OpKind 是 DDL 操作类型。
type OpKind int

const (
	OpCreateTable OpKind = iota + 1
	OpDropTable
	OpCreateIndex
	OpDropIndex
)

// Op 是一条解析完成并校验通过的 DDL 操作。
type Op struct {
	Kind    OpKind
	Table   string
	Def     *meta.TableDef // OpCreateTable
	Index   *meta.IndexDef // OpCreateIndex
	IndexID string         // OpDropIndex
}

// tableOpts 是 CREATE TABLE COMMENT 的 kidb payload（docs/02 §2.4）。
//
// 纪律（docs/01 §1.0 设计原点）：payload 只留**语义声明**。
// 调优/容量类选项全部自动或内置——max_row_bytes 固定 1MB（tuning.toml），
// exp 登记册细分自动（后续自治项），维表判定按实时行数，字典序副本自动。
// 严格解析：未知字段报错（"不支持直接报错"优于静默忽略）。
type tableOpts struct {
	DefaultTTL int64 `json:"default_ttl"` // 行级 TTL 语义（缓存定位的核心语义）
}

// indexOpts 是索引 COMMENT 的 kidb payload（同样只留语义声明）。
type indexOpts struct {
	Covering []string `json:"covering"` // 覆盖列（存储-延迟权衡的物理设计选择，docs/03 §3.5）
	Async    bool     `json:"async"`    // 异步索引（最终一致窗口的一致性选择，docs/05 §5.2）
}

// kidbCommentPrefix 是 KiDB 扩展在 COMMENT 中的前缀。
// 非此前缀的 COMMENT 视为普通注释，两不干扰。
const kidbCommentPrefix = "kidb:"

// Parse 解析并校验一条 DDL 语句。调用方保证语句已过前置分类器
// （gateway.Classify == RouteDDL，docs/02 §2.2）。
func Parse(query string) (*Op, error) {
	p := parser.New()
	stmts, _, err := p.Parse(query, "", "")
	if err != nil {
		return nil, fmt.Errorf("ddl parse: %w", err)
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("%w: expect single DDL statement, got %d", kidb.ErrUnsupported, len(stmts))
	}
	switch s := stmts[0].(type) {
	case *ast.CreateTableStmt:
		return buildCreateTable(s)
	case *ast.CreateIndexStmt:
		return buildCreateIndex(s)
	case *ast.DropTableStmt:
		if len(s.Tables) != 1 {
			return nil, fmt.Errorf("%w: DROP TABLE 限单表", kidb.ErrUnsupported)
		}
		return &Op{Kind: OpDropTable, Table: s.Tables[0].Name.O}, nil
	case *ast.DropIndexStmt:
		return &Op{Kind: OpDropIndex, Table: s.Table.Name.O, IndexID: s.IndexName}, nil
	case *ast.AlterTableStmt:
		return buildAlterTable(s)
	case *ast.TruncateTableStmt:
		return nil, fmt.Errorf("%w: TRUNCATE 为无界操作，用 TTL 或 DROP+重建（docs/02 §2.4）", kidb.ErrUnsupported)
	}
	return nil, fmt.Errorf("%w: 不支持的 DDL 形态 %T", kidb.ErrUnsupported, stmts[0])
}

// buildAlterTable 仅放行 ADD/DROP INDEX（映射独立作业）；其余形态报错。
func buildAlterTable(s *ast.AlterTableStmt) (*Op, error) {
	if len(s.Specs) != 1 {
		return nil, fmt.Errorf("%w: ALTER TABLE 限单动作", kidb.ErrUnsupported)
	}
	spec := s.Specs[0]
	switch spec.Tp {
	case ast.AlterTableAddConstraint:
		idx, err := constraintToIndex(spec.Constraint)
		if err != nil {
			return nil, err
		}
		return &Op{Kind: OpCreateIndex, Table: s.Table.Name.O, Index: idx}, nil
	case ast.AlterTableDropIndex:
		return &Op{Kind: OpDropIndex, Table: s.Table.Name.O, IndexID: spec.Name}, nil
	}
	return nil, fmt.Errorf("%w: ALTER TABLE 仅支持 ADD/DROP INDEX（docs/02 §2.4）", kidb.ErrUnsupported)
}

// buildCreateTable 建表：列白名单 + 主键 + 内联索引 + kidb 表选项。
func buildCreateTable(s *ast.CreateTableStmt) (*Op, error) {
	if s.Select != nil || s.ReferTable != nil {
		return nil, fmt.Errorf("%w: CREATE TABLE ... SELECT/LIKE", kidb.ErrUnsupported)
	}
	name := s.Table.Name.O
	def := &meta.TableDef{Name: name}

	// 列
	if len(s.Cols) == 0 || len(s.Cols) > 256 {
		return nil, fmt.Errorf("%w: 列数 %d 超出 [1,256]", kidb.ErrUnsupported, len(s.Cols))
	}
	seen := map[string]bool{}
	for _, c := range s.Cols {
		cn := c.Name.Name.O
		if err := meta.ValidateReserved(cn); err != nil {
			return nil, fmt.Errorf("%w: %v", kidb.ErrUnsupported, err)
		}
		if seen[strings.ToLower(cn)] {
			return nil, fmt.Errorf("列 %q 重复", cn)
		}
		seen[strings.ToLower(cn)] = true
		ct, err := mapColumnType(c.Tp)
		if err != nil {
			return nil, err
		}
		col := meta.ColumnDef{Name: cn, Type: ct}
		for _, o := range c.Options {
			switch o.Tp {
			case ast.ColumnOptionNotNull:
				col.NotNull = true
			case ast.ColumnOptionPrimaryKey:
				if def.PK != "" {
					return nil, fmt.Errorf("%w: 复合/多主键", kidb.ErrUnsupported)
				}
				def.PK = cn
			case ast.ColumnOptionAutoIncrement:
				def.AutoIncrColumn = cn
			case ast.ColumnOptionDefaultValue, ast.ColumnOptionNull:
				// 默认值 v1 不取（docs/02 §2.4 未声明）；NULL 语义由行缺失字段表达
			default:
				return nil, fmt.Errorf("%w: 列 %q 含不支持的列选项 %d", kidb.ErrUnsupported, cn, o.Tp)
			}
		}
		def.Columns = append(def.Columns, col)
	}

	// 表级约束（PRIMARY KEY / INDEX / UNIQUE）
	for _, con := range s.Constraints {
		switch con.Tp {
		case ast.ConstraintPrimaryKey:
			if def.PK != "" {
				return nil, fmt.Errorf("%w: 复合/多主键", kidb.ErrUnsupported)
			}
			cols, err := constraintColumns(con)
			if err != nil {
				return nil, err
			}
			if len(cols) != 1 {
				return nil, fmt.Errorf("%w: 主键限单列", kidb.ErrUnsupported)
			}
			def.PK = cols[0]
		case ast.ConstraintIndex, ast.ConstraintKey, ast.ConstraintUniq:
			idx, err := constraintToIndex(con)
			if err != nil {
				return nil, err
			}
			def.Indexes = append(def.Indexes, *idx)
		case ast.ConstraintForeignKey:
			return nil, fmt.Errorf("%w: 外键超出缓存查询层定位", kidb.ErrUnsupported)
		default:
			return nil, fmt.Errorf("%w: 不支持的约束类型 %d", kidb.ErrUnsupported, con.Tp)
		}
	}

	// kidb 表选项（COMMENT 载体；其余表选项如 ENGINE/CHARSET 忽略——KiDB 无此概念）
	if to := findTableOpts(s.Options); to != nil {
		def.DefaultTTL = to.DefaultTTL
	}
	if err := validateTable(def); err != nil {
		return nil, err
	}
	return &Op{Kind: OpCreateTable, Table: name, Def: def}, nil
}

// buildCreateIndex 独立建索引语句。
func buildCreateIndex(s *ast.CreateIndexStmt) (*Op, error) {
	switch s.KeyType {
	case ast.IndexKeyTypeNone, ast.IndexKeyTypeUnique:
	case ast.IndexKeyTypeFulltext, ast.IndexKeyTypeSpatial:
		return nil, fmt.Errorf("%w: FULLTEXT/SPATIAL 超出缓存查询层定位", kidb.ErrUnsupported)
	default:
		return nil, fmt.Errorf("%w: 索引类型 %d", kidb.ErrUnsupported, s.KeyType)
	}
	cols, err := indexPartColumns(s.IndexPartSpecifications)
	if err != nil {
		return nil, err
	}
	idx := &meta.IndexDef{ID: s.IndexName, Columns: cols}
	if s.KeyType == ast.IndexKeyTypeUnique {
		idx.Kind = meta.IndexUnique
	}
	if s.IndexOption != nil && s.IndexOption.Comment != "" {
		opts, err := parseIndexOpts(s.IndexOption.Comment)
		if err != nil {
			return nil, err
		}
		idx.Covering = opts.Covering
		idx.Async = opts.Async
	}
	if err := validateIndexShape(idx); err != nil {
		return nil, err
	}
	// 类型相关校验需要表定义，由 DDL 作业执行时结合 Catalog 完成（docs/06 §6.3）。
	return &Op{Kind: OpCreateIndex, Table: s.Table.Name.O, Index: idx}, nil
}

// constraintToIndex 表内联索引约束 → IndexDef。
func constraintToIndex(con *ast.Constraint) (*meta.IndexDef, error) {
	cols, err := constraintColumns(con)
	if err != nil {
		return nil, err
	}
	idx := &meta.IndexDef{ID: con.Name, Columns: cols}
	if con.Tp == ast.ConstraintUniq {
		idx.Kind = meta.IndexUnique
	}
	if con.Option != nil && con.Option.Comment != "" {
		opts, err := parseIndexOpts(con.Option.Comment)
		if err != nil {
			return nil, err
		}
		idx.Covering = opts.Covering
		idx.Async = opts.Async
	}
	if idx.ID == "" {
		return nil, fmt.Errorf("%w: 索引必须命名", kidb.ErrUnsupported)
	}
	if err := validateIndexShape(idx); err != nil {
		return nil, err
	}
	return idx, nil
}

// constraintColumns 取约束列名列表（仅列引用，不支持表达式索引）。
func constraintColumns(con *ast.Constraint) ([]string, error) {
	return indexPartColumns(con.Keys)
}

func indexPartColumns(parts []*ast.IndexPartSpecification) ([]string, error) {
	var cols []string
	for _, p := range parts {
		if p.Column == nil {
			return nil, fmt.Errorf("%w: 表达式索引不支持", kidb.ErrUnsupported)
		}
		if p.Length > 0 {
			return nil, fmt.Errorf("%w: 前缀长度索引不支持（前缀搜索由字典序副本自动承担）", kidb.ErrUnsupported)
		}
		cols = append(cols, p.Column.Name.O)
	}
	return cols, nil
}

// mapColumnType 列类型白名单映射（docs/02 §2.4）。
func mapColumnType(tp *ptypes.FieldType) (meta.ColumnType, error) {
	if tp == nil {
		return 0, fmt.Errorf("%w: 缺列类型", kidb.ErrUnsupported)
	}
	switch tp.GetType() {
	case pmysql.TypeLonglong, pmysql.TypeLong, pmysql.TypeShort, pmysql.TypeTiny,
		pmysql.TypeInt24, pmysql.TypeYear, pmysql.TypeBit:
		return meta.ColInt, nil
	case pmysql.TypeDouble, pmysql.TypeFloat, pmysql.TypeNewDecimal:
		return meta.ColFloat, nil
	case pmysql.TypeVarchar, pmysql.TypeString, pmysql.TypeVarString:
		return meta.ColString, nil
	case pmysql.TypeBlob, pmysql.TypeTinyBlob, pmysql.TypeMediumBlob, pmysql.TypeLongBlob:
		return meta.ColBytes, nil
	case pmysql.TypeDatetime, pmysql.TypeTimestamp, pmysql.TypeDate:
		return meta.ColTimestamp, nil
	case pmysql.TypeJSON:
		return meta.ColJSON, nil
	}
	return 0, fmt.Errorf("%w: 列类型码 %d 不在白名单（docs/02 §2.4）", kidb.ErrUnsupported, tp.GetType())
}

// validateTable 建表校验（docs/02 §2.4 校验规则）。
func validateTable(t *meta.TableDef) error {
	if t.PK == "" {
		return fmt.Errorf("%w: 必须显式声明单列主键", kidb.ErrUnsupported)
	}
	pkCol, ok := t.Column(t.PK)
	if !ok {
		return fmt.Errorf("主键列 %q 不存在", t.PK)
	}
	if pkCol.Type != meta.ColInt && pkCol.Type != meta.ColString {
		return fmt.Errorf("%w: 主键类型限 INT/STRING", kidb.ErrUnsupported)
	}
	if t.AutoIncrColumn != "" && (t.AutoIncrColumn != t.PK || pkCol.Type != meta.ColInt) {
		return fmt.Errorf("%w: AUTO_INCREMENT 限 INT 主键列（docs/05 §5.4）", kidb.ErrUnsupported)
	}
	if len(t.Indexes) > 16 {
		return fmt.Errorf("%w: 单表索引数 %d > 16", kidb.ErrUnsupported, len(t.Indexes))
	}
	seen := map[string]bool{}
	for i := range t.Indexes {
		idx := &t.Indexes[i]
		if err := meta.ValidateReserved(idx.ID); err != nil {
			return fmt.Errorf("%w: %v", kidb.ErrUnsupported, err)
		}
		if seen[strings.ToLower(idx.ID)] {
			return fmt.Errorf("索引名 %q 重复", idx.ID)
		}
		seen[strings.ToLower(idx.ID)] = true
		if err := validateIndexAgainstTable(idx, t); err != nil {
			return err
		}
	}
	return nil
}

// validateIndexShape 索引形态校验（不依赖表定义的部分）。
func validateIndexShape(idx *meta.IndexDef) error {
	if len(idx.Columns) != 1 {
		return fmt.Errorf("%w: 索引限单列（%s 有 %d 列）", kidb.ErrUnsupported, idx.ID, len(idx.Columns))
	}
	if idx.Async && idx.Kind == meta.IndexUnique {
		return fmt.Errorf("%w: 唯一索引必须同步模式（%s）", kidb.ErrUnsupported, idx.ID)
	}
	if idx.Async && len(idx.Covering) > 0 {
		return fmt.Errorf("%w: 异步索引不允许 COVERING（docs/03 §3.5，%s）", kidb.ErrUnsupported, idx.ID)
	}
	return nil
}

// validateIndexAgainstTable 索引对表校验（类型相关）。
func validateIndexAgainstTable(idx *meta.IndexDef, t *meta.TableDef) error {
	if err := validateIndexShape(idx); err != nil {
		return err
	}
	col, ok := t.Column(idx.Columns[0])
	if !ok {
		return fmt.Errorf("索引列 %q 不存在", idx.Columns[0])
	}
	// 形态推导（docs/02 §2.4）：唯一=UNIQUE；数值/时间戳=RANGE；其余=等值。
	if idx.Kind != meta.IndexUnique {
		if col.Type.RangeIndexable() {
			idx.Kind = meta.IndexRange
		} else {
			idx.Kind = meta.IndexEq
		}
	}
	// 字典序副本自动开启（docs/01 §1.0：前缀搜索开箱即用，无需声明）——
	// 字符串等值/唯一索引自带 #l 副本（写放大 1 ZADD/写，收益是 LIKE 'abc%' 直答）
	if idx.Kind != meta.IndexRange && col.Type == meta.ColString {
		idx.PrefixCopy = true
	}
	for _, cc := range idx.Covering {
		cdef, ok := t.Column(cc)
		if !ok {
			return fmt.Errorf("覆盖列 %q 不存在", cc)
		}
		// 覆盖列必须 NOT NULL：member 是 msgp 字符串数组，NULL 无法保真
		// （读路径会把 NULL 返回成空串——静默错值，违反结果精确纪律）。
		// 需要可空覆盖列的场景：回表路径本就正确，不声明 covering 即可。
		if !cdef.NotNull {
			return fmt.Errorf("%w: 覆盖列 %q 必须 NOT NULL（member 编码 NULL 不保真，docs/03 §3.5）", kidb.ErrUnsupported, cc)
		}
	}
	return nil
}

// ValidateIndexForTable 是 CREATE INDEX 作业执行时的表级校验入口（docs/06 §6.3）。
func ValidateIndexForTable(idx *meta.IndexDef, t *meta.TableDef) error {
	return validateIndexAgainstTable(idx, t)
}

// findTableOpts 从表选项中取 kidb payload（无则 nil）。
// COMMENT 在 AST 中落 StrValue（字符串字面量）；保守兜底 Value.GetString()。
func findTableOpts(opts []*ast.TableOption) *tableOpts {
	for _, o := range opts {
		if o.Tp != ast.TableOptionComment {
			continue
		}
		v := o.StrValue
		if v == "" && o.Value != nil {
			v = o.Value.GetString()
		}
		to, err := parseTableOpts(v)
		if err == nil && to != nil {
			return to
		}
	}
	return nil
}

func parseTableOpts(comment string) (*tableOpts, error) {
	v, ok := strings.CutPrefix(comment, kidbCommentPrefix)
	if !ok {
		return nil, nil
	}
	var to tableOpts
	if err := func() error {
		d := json.NewDecoder(strings.NewReader(v))
		d.DisallowUnknownFields()
		return d.Decode(&to)
	}(); err != nil {
		return nil, fmt.Errorf("kidb 表选项 JSON 非法: %w", err)
	}
	return &to, nil
}

func parseIndexOpts(comment string) (*indexOpts, error) {
	v, ok := strings.CutPrefix(comment, kidbCommentPrefix)
	if !ok {
		return &indexOpts{}, nil
	}
	var io indexOpts
	dec := json.NewDecoder(strings.NewReader(v))
	dec.DisallowUnknownFields() // 严格解析：未知/已移除字段报错（docs/01 §1.0）
	if err := dec.Decode(&io); err != nil {
		return nil, fmt.Errorf("kidb 索引选项 JSON 非法: %w", err)
	}
	return &io, nil
}
