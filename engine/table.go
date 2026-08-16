package engine

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"kidb"
	"kidb/keycodec"
	"kidb/exec"
	"kidb/i18n"
	"kidb/meta"
)

// Table 实现 gms 的 Table/IndexedTable/IndexAddressable/PrimaryKey/Statistics/
// Insertable/Updatable/Deletable/Replaceable/AutoIncrement/ProjectedTable 组合接口，
// 全部落在 Catalog 定义 + exec 执行器 + txguard 写入上（docs/02 §2.5 DML 路径）。
type Table struct {
	def  *meta.TableDef
	deps Deps

	// proj 投影列（gms pruneTables 下推，schema 序子集；projected=false 时无意义）。
	// 契约：Schema() 只含投影列，行与投影同宽同序（exec.DecodeRowCols）。
	proj      []string
	projected bool

	// stats 行数统计缓存（指针：WithProjections 的表拷贝共享同一统计——
	// 同一底层表语义正确；且规避拷贝 sync.Mutex 的 vet/竞态问题）。
	stats *tableStats
}

// tableStats RowCount 的 1s 统计缓存（挡分析器高频调用）。
type tableStats struct {
	mu    sync.Mutex
	at    time.Time
	count uint64
}

// NewTable 构造。
func NewTable(def *meta.TableDef, d Deps) *Table {
	return &Table{def: def, deps: d, stats: &tableStats{}}
}

// Name 表名。
func (t *Table) Name() string { return t.def.Name }

// String 同 Name。
func (t *Table) String() string { return t.def.Name }

// Schema 由 Catalog 定义生成 gms schema（pk 列打 PrimaryKey 标记）。
// 投影态下只含投影列（ProjectedTable 契约，列序保持表 schema 序）。
func (t *Table) Schema() sql.Schema {
	if !t.projected {
		return t.fullSchema()
	}
	sch := make(sql.Schema, 0, len(t.proj))
	for _, c := range t.fullSchema() {
		if t.inProjection(c.Name) {
			sch = append(sch, c)
		}
	}
	return sch
}

// fullSchema 全量 schema（投影无关）。
// 忠实类型重建（docs/02 §2.2）：列类型按 Catalog 规范类型文本还原
// （varchar 长度不丢）；文本非法 = Catalog 写坏（唯一写入点 TableFromSchema），
// 属内核契约违例，fail fast。
func (t *Table) fullSchema() sql.Schema {
	sch := make(sql.Schema, 0, len(t.def.Columns))
	for _, c := range t.def.Columns {
		gt, err := columnTypeFromText(c.TypeText)
		if err != nil {
			panic(fmt.Sprintf("engine: 表 %s 列 %s 类型文本非法: %v", t.def.Name, c.Name, err))
		}
		sch = append(sch, &sql.Column{
			Name:          c.Name,
			Type:          gt,
			Nullable:      !c.NotNull,
			PrimaryKey:    c.Name == t.def.PK,
			Source:        t.def.Name,
			AutoIncrement: t.def.AutoIncrColumn == c.Name,
		})
	}
	return sch
}

// inProjection 列是否在投影内（大小写不敏感——gms 列名大小写不保证与 DDL 一致）。
func (t *Table) inProjection(name string) bool {
	for _, p := range t.proj {
		if strings.EqualFold(p, name) {
			return true
		}
	}
	return false
}

// WithProjections 投影下推（sql.ProjectedTable）：行/Schema 只含指定列。
// 空切片合法（零宽行，COUNT 类只数行）；nil 不会传入（gms 契约）。
func (t *Table) WithProjections(colNames []string) sql.Table {
	nt := *t
	nt.proj = colNames
	nt.projected = true
	return &nt
}

// Projections 当前投影（nil = 未投影）。
func (t *Table) Projections() []string {
	if !t.projected {
		return nil
	}
	return t.proj
}

// projectionForExec 传给 exec 的输出列（nil = 全列）。
func (t *Table) projectionForExec() []string {
	if !t.projected {
		return nil
	}
	return t.proj
}

// Collation 默认 utf8mb4 排序规则。
func (t *Table) Collation() sql.CollationID { return sql.Collation_Default }

// PrimaryKeySchema 主键 schema（**恒为全量 schema**，投影也不例外）：
// gms coster（uniformDistStatisticsForIndex 的 indexFds）要求索引列在
// PrimaryKeySchema().Schema 中——投影窄 schema 会让 PRIMARY 索引列缺失、
// 统计构造返回 nil stat 后在 gms 内部 panic（实证）；且 fix_exec_indexes.go:648
// 显式兼容 PrimaryKeySchema 比节点 schema 宽的情形（dolt 同形）。
func (t *Table) PrimaryKeySchema() sql.PrimaryKeySchema {
	sch := t.fullSchema()
	for i, c := range sch {
		if c.PrimaryKey {
			return sql.NewPrimaryKeySchema(sch, i)
		}
	}
	return sql.NewPrimaryKeySchema(sch)
}

// kidbPartition 单一逻辑分区（桶散取在 RowIter 内部分页，docs/04 §4.3）。
type kidbPartition struct{ key []byte }

// Key 分区键。
func (p kidbPartition) Key() []byte { return p.key }

var thePartition = kidbPartition{key: []byte("kidb")}

// Partitions 单分区（执行单元在 PartitionRows 内流式分页）。
func (t *Table) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(thePartition), nil
}

// PartitionRows 全表遍历（exp 登记册驱动，docs/07 §7.4）；
// 若分区携带编译后的索引计划（lookupPartition），执行索引路径。
// 全扫闸门（v6.0 引擎层执法，docs/04 §4.4）：小表自动放行 / 白名单放行并告警 /
// 否则 ERR_NO_INDEX——无索引谓词纪律的唯一落点（不再有第二套判定）。
func (t *Table) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	if lp, ok := part.(lookupPartition); ok {
		return t.streamFor(ctx, lp.req), nil
	}
	gate := t.deps.FullscanGate
	if gate == nil {
		// 未装配 = 保守拒绝（docs/01 §1.0：默认取保守安全值；生产由装配层注入）
		return nil, sqlErr(fmt.Errorf("%w: %s", kidb.ErrNoIndex, i18n.T("err.gate_unwired", t.def.Name)))
	}
	n, _, err := t.RowCount(ctx)
	if err != nil {
		return nil, err
	}
	if err := gate(ctx, t.def.Name, n); err != nil {
		return nil, sqlErr(err)
	}
	req := &exec.Request{Table: t.def, Kind: exec.FullScan, Projection: t.projectionForExec()}
	return &streamIter{s: t.deps.Exec.Run(ctx, req)}, nil
}

// LookupPartitions 索引查询：把 gms IndexLookup 翻译为 KiDB 物理计划（docs/04 §4.1）。
func (t *Table) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	req, err := t.translateLookup(lookup)
	if err != nil {
		return nil, err
	}
	return &lookupIter{req: req}, nil
}

// lookupIter 携带编译后计划的单分区迭代器。
type lookupIter struct {
	req  *exec.Request
	done bool
}

func (i *lookupIter) Next(*sql.Context) (sql.Partition, error) {
	if i.done {
		return nil, io.EOF
	}
	i.done = true
	return lookupPartition{req: i.req}, nil
}

func (i *lookupIter) Close(*sql.Context) error { return nil }

// lookupPartition 携带请求的分区（PartitionRows 按类型取回）。
type lookupPartition struct{ req *exec.Request }

func (p lookupPartition) Key() []byte { return []byte(p.req.Table.Name) }

// PartitionRowsFor 供 engine 内部使用：执行编译后的计划。
func (t *Table) streamFor(ctx *sql.Context, req *exec.Request) sql.RowIter {
	return &streamIter{s: t.deps.Exec.Run(ctx, req)}
}

// streamIter 把 exec.RowStream 适配为 sql.RowIter（全链路流式，docs/04 §4.3）。
type streamIter struct {
	s *exec.RowStream
}

func (i *streamIter) Next(ctx *sql.Context) (sql.Row, error) {
	return i.s.Next()
}

func (i *streamIter) Close(ctx *sql.Context) error { return i.s.Close() }

// GetIndexes 暴露二级索引 + 隐式 PRIMARY（docs/04 §4.1 翻译表的索引面）。
func (t *Table) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	out := make([]sql.Index, 0, len(t.def.Indexes)+1)
	out = append(out, &Index{id: "PRIMARY", table: t.def.Name, cols: []string{t.def.PK}, unique: true, primary: true, deps: t.deps, def: t.def})
	for _, idx := range t.def.Indexes {
		if idx.Building {
			continue // 回填中：查询不可见（docs/06 §6.3；写入路径仍双写覆盖回填窗口）
		}
		out = append(out, &Index{
			id: idx.ID, table: t.def.Name, cols: idx.Columns,
			unique: idx.Kind == meta.IndexUnique, deps: t.deps, def: t.def,
		})
	}
	return out, nil
}

// RowCount 精确行数（Σ ZCOUNT(exp, (now, +inf))；1s 统计缓存挡分析器高频调用）。
func (t *Table) RowCount(ctx *sql.Context) (uint64, bool, error) {
	t.stats.mu.Lock()
	defer t.stats.mu.Unlock()
	if time.Since(t.stats.at) < time.Second && t.stats.at.Unix() > 0 {
		return t.stats.count, true, nil
	}
	n, err := t.deps.Exec.RowCount(ctx, t.def, time.Now().Unix())
	if err != nil {
		return 0, false, err
	}
	t.stats.at = time.Now()
	t.stats.count = n
	return n, true, nil // 任意时刻精确（docs/04 §4.6 纪律：COUNT 精确）
}

// DataLength 估算（统计用途）。
func (t *Table) DataLength(ctx *sql.Context) (uint64, error) {
	n, _, err := t.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return n * 200, nil // 均值 200B/行（docs/03 §3.6 估算口径）
}

// GetNextAutoIncrementValue 取 AUTO_INCREMENT 序列值（docs/05 §5.4）。
func (t *Table) GetNextAutoIncrementValue(ctx *sql.Context, insertVal interface{}) (uint64, error) {
	return t.deps.Guard.NextAutoID(ctx, t.def.Name)
}

// PeekNextAutoIncrementValue 窥探下一值。
func (t *Table) PeekNextAutoIncrementValue(ctx *sql.Context) (uint64, error) {
	return t.deps.Guard.NextAutoID(ctx, t.def.Name) // 窥探即消费：缓存语义下可接受（空洞允许）
}

// AutoIncrementSetter 自增写入器。
func (t *Table) AutoIncrementSetter(ctx *sql.Context) sql.AutoIncrementSetter {
	return &autoIncr{t: t}
}

type autoIncr struct{ t *Table }

// AcquireAutoIncrementLock 返回 no-op 释放函数（并发安全由 INCR 原子性保证）。
func (a *autoIncr) AcquireAutoIncrementLock(ctx *sql.Context) (func(), error) {
	return func() {}, nil
}

// SetAutoIncrementValue 设置序列当前值（ALTER AUTO_INCREMENT / 导入用）。
func (a *autoIncr) SetAutoIncrementValue(ctx *sql.Context, val uint64) error {
	_, err := a.t.deps.Client.Do(ctx, "SET", keycodec.SeqKey(a.t.def.Name), val)
	return err
}

// Close 关闭。
func (a *autoIncr) Close(*sql.Context) error { return nil }

// 编译期接口断言。
var (
	_ sql.Table                 = (*Table)(nil)
	_ sql.IndexedTable          = (*Table)(nil)
	_ sql.IndexAddressableTable = (*Table)(nil)
	_ sql.PrimaryKeyTable       = (*Table)(nil)
	_ sql.StatisticsTable       = (*Table)(nil)
	_ sql.InsertableTable       = (*Table)(nil)
	_ sql.UpdatableTable        = (*Table)(nil)
	_ sql.DeletableTable        = (*Table)(nil)
	_ sql.ReplaceableTable      = (*Table)(nil)
	_ sql.AutoIncrementTable    = (*Table)(nil)
	_ sql.ProjectedTable        = (*Table)(nil)
)

// IndexedAccess 返回绑定指定 lookup 的索引访问视图（sql.IndexAddressable）。
func (t *Table) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	return &indexedTable{Table: t}
}

// PreciseMatch 索引访问可替代过滤（回表校验保证索引路径精确，docs/04 §4.3）。
func (t *Table) PreciseMatch() bool { return true }

// indexedTable 是 IndexedAccess 返回的视图（LookupPartitions 仍按传入 lookup 翻译，
// gms 会把同一 lookup 再传进来）。
type indexedTable struct{ *Table }

var _ sql.IndexAddressable = (*Table)(nil)
