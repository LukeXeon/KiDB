package engine

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"kidb"
	"kidb/exec"
	"kidb/i18n"
	"kidb/keycodec"
	"kidb/meta"
	"kidb/rowcodec"
	"kidb/tuning"
)

// ddlexec.go：DDL 执行（docs/06 §6.3 作业流）——gms 引擎的 DDL 接口实现。
// Database: sql.TableCreator/TableDropper；Table: sql.IndexAlterableTable。
// 全部经 gms 引擎驱动（类型语义以 gms 为准），KiDB 侧只做定义校验 + Catalog 作业。
//
// 正确性论证（继承自网关直执行时代，语义不变）：
// Catalog 先落库 → 并发写入即双写新索引；回填按当前行值 ZADD，
// ZSet 去重天然幂等；回填与删除交错的残留由回表校验兜底（docs/01 §1.7）。

// CreateTable 建表（sql.TableCreator）：ro 执法 → 校验 → Catalog 落库 + 注册。
func (d *Database) CreateTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID, comment string) error {
	return sqlErr(d.createTable(ctx, name, schema, collation, comment))
}

func (d *Database) createTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID, comment string) error {
	if err := RejectRO(ctx); err != nil {
		return err
	}
	def, err := TableFromSchema(name, schema, comment)
	if err != nil {
		return err
	}
	cur, err := d.p.deps.Store.Load(ctx, name)
	if err != nil {
		return err
	}
	if cur != nil {
		return sql.ErrTableAlreadyExists.New(name)
	}
	// DROP 清理作业在途的同名表拒绝重建（review 实证：不挡住则旧作业被
	// "Catalog 在" 防护永久卡死，且旧代行与新表同 key 命名空间——幽灵行
	// 对新表可见）。作业完成后（秒级~分钟级）重试即可。
	if jobs, err := d.p.deps.Store.ListDropJobs(ctx); err == nil {
		for _, j := range jobs {
			if j.Table == name {
				return fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.dropjob_in_progress", name))
			}
		}
	}
	if err := d.p.deps.Store.Save(ctx, def, 0); err != nil {
		return err
	}
	if err := d.p.deps.Store.RegisterTable(ctx, name); err != nil {
		return err
	}
	d.p.deps.Cache.Invalidate()
	return nil
}

// DropTable 删表（sql.TableDropper）：行数 ≤ drop_sync_max_rows 同步清理
// （逐行 DeleteRow 全清语义）；超出走 c:dropjobs 后台清理作业（下文大表分支）。
func (d *Database) DropTable(ctx *sql.Context, name string) error {
	return sqlErr(d.dropTable(ctx, name))
}

func (d *Database) dropTable(ctx *sql.Context, name string) error {
	if err := RejectRO(ctx); err != nil {
		return err
	}
	deps := d.p.deps
	def, err := deps.Store.Load(ctx, name)
	if err != nil {
		return err
	}
	if def == nil {
		return sql.ErrTableNotFound.New(name)
	}

	// 大表作业化（docs/06 §6.3）：行数超阈值 → Catalog 立即删除（表即刻不可
	// 查询/写入）+ 登记 c:dropjobs 清理作业（def 快照自包含），JobRunner 后台
	// 按 slot 游标清理。顺序纪律：先登记作业后删 Catalog——崩溃窗口内作业存在
	// 但 Catalog 未删时 JobRunner 跳过（在途防护），DROP 重试幂等续作。
	n, _, err := NewTable(def, deps).RowCount(ctx)
	if err != nil {
		return err
	}
	big := n > uint64(tuning.Get().Controller.DropSyncMaxRows)
	hasJob := false
	if jobs, jerr := deps.Store.ListDropJobs(ctx); jerr == nil {
		for _, j := range jobs {
			if j.Table == name {
				hasJob = true
			}
		}
	}
	if big || hasJob {
		if !hasJob {
			if err := deps.Store.SetDropJob(ctx, &meta.DropJob{
				Table: name, Cursor: 0, Started: time.Now().Unix(), Def: def,
			}); err != nil {
				return err
			}
		}
		if _, err := deps.Client.Do(ctx, "DEL", keycodec.CatalogKey(name)); err != nil {
			return err
		}
		if err := deps.Store.UnregisterTable(ctx, name); err != nil {
			return err
		}
		if _, err := deps.Client.Do(ctx, "INCR", keycodec.SchemaVerKey()); err != nil {
			return err
		}
		deps.Cache.Invalidate()
		return nil // 清理由 JobRunner 后台完成（ddl_job_duration_seconds{type=drop_table}）
	}

	s := deps.Exec.Run(ctx, &exec.Request{Table: def, Kind: exec.FullScan})
	defer s.Close()
	for {
		row, err := s.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if _, err := deps.Guard.DeleteRow(ctx, def, pkOf(def, row)); err != nil {
			return err
		}
	}
	if _, err := deps.Client.Do(ctx, "DEL", keycodec.CatalogKey(name)); err != nil {
		return err
	}
	if err := deps.Store.UnregisterTable(ctx, name); err != nil {
		return err
	}
	if _, err := deps.Client.Do(ctx, "INCR", keycodec.SchemaVerKey()); err != nil {
		return err
	}
	deps.Cache.Invalidate()
	return nil
}

// CreateIndex 建索引（sql.IndexAlterableTable——内联索引/ALTER ADD INDEX/
// 独立 CREATE INDEX 统一入口，docs/06 §6.3 作业流）：
// 空表同步完成（CREATE TABLE 内联索引场景，绕开单 _job 槽位）；
// 非空表走 _job 后台回填（Building 标记，完成前查询不可见，写入双写覆盖窗口）。
func (t *Table) CreateIndex(ctx *sql.Context, idxDef sql.IndexDef) error {
	return sqlErr(t.createIndex(ctx, idxDef))
}

func (t *Table) createIndex(ctx *sql.Context, idxDef sql.IndexDef) error {
	if err := RejectRO(ctx); err != nil {
		return err
	}
	idx, err := IndexFromDef(idxDef)
	if err != nil {
		return err
	}
	deps := t.deps
	// 重新加载当前定义（本 Table 实例可能是过期快照——lease 窗口）
	def, err := deps.Store.Load(ctx, t.def.Name)
	if err != nil {
		return err
	}
	if def == nil {
		return sql.ErrTableNotFound.New(t.def.Name)
	}
	if def.Index(idx.ID) != nil {
		return fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.index_exists", idx.ID))
	}
	if err := ValidateIndexForTable(idx, def); err != nil {
		return err
	}

	// 唯一索引存量查重（MySQL 语义：存量重复值拒建——ERROR 1062）。
	// 同步全扫流式去重；去重集超上限拒建（诚实红线，docs/14）。
	// 查重与并发写入的交错窗口由回填期预约冲突中止兜底（docs/06 §6.3）。
	if idx.Kind == meta.IndexUnique {
		if err := t.checkUniqueNoDup(ctx, def, idx); err != nil {
			return err
		}
	}

	// 先以 Building 追加（写入路径即刻双写覆盖——无数据空洞窗口）
	idxCopy := *idx
	idxCopy.Building = true
	def.Indexes = append(def.Indexes, idxCopy)
	if err := deps.Store.Save(ctx, def, def.Ver); err != nil {
		return err
	}
	def.Ver++ // Save 成功即版本递增——后续同函数内再 Save 必须换基线

	// 空表快速通道：无回填对象，直接转可见（无需作业）
	n, err := deps.Exec.RowCount(ctx, def)
	if err != nil {
		return err
	}
	if n == 0 {
		for i := range def.Indexes {
			if def.Indexes[i].ID == idx.ID {
				def.Indexes[i].Building = false
			}
		}
		if err := deps.Store.Save(ctx, def, def.Ver); err != nil {
			return err
		}
		if _, err := deps.Client.Do(ctx, "INCR", keycodec.SchemaVerKey()); err != nil {
			return err
		}
		deps.Cache.Invalidate()
		return nil
	}

	// 非空表：单 _job 槽位（表内有进行中作业则拒绝——DDL 低频管理面）
	if job, err := deps.Store.GetJob(ctx, def.Name); err != nil {
		return err
	} else if job != nil {
		return fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.job_in_progress", def.Name))
	}
	if err := deps.Store.SetJob(ctx, def.Name, &meta.DDLJob{
		Type: "create_index", Index: &idxCopy, Cursor: 0, Started: time.Now().Unix(),
	}); err != nil {
		return err
	}
	deps.Cache.Invalidate()
	return nil
}

// checkUniqueNoDup 唯一索引存量查重：全扫投影单列，流式去重；
// 命中重复 → 1062；distinct 超 l2_max_collect → 拒建（内存有界红线）。
// NULL 不入唯一约束（MySQL 语义：多 NULL 合法——写路径对 NULL 不建预约同源）。
func (t *Table) checkUniqueNoDup(ctx *sql.Context, def *meta.TableDef, idx *meta.IndexDef) error {
	col := idx.Columns[0]
	colDef, ok := def.Column(col)
	if !ok {
		return fmt.Errorf("%s", i18n.T("err.column_missing", col))
	}
	s := t.deps.Exec.Run(ctx, &exec.Request{Table: def, Kind: exec.FullScan, Projection: []string{col}})
	defer s.Close()
	seen := make(map[string]struct{}, 1024)
	cap_ := tuning.Get().Exec.L2MaxCollect
	for {
		row, err := s.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if len(row) == 0 || row[0] == nil {
			continue
		}
		enc, err := rowcodec.Encode(colDef.Type, row[0])
		if err != nil {
			return err
		}
		if _, dup := seen[enc]; dup {
			return sql.NewUniqueKeyErr(fmt.Sprintf("%s on %s", enc, idx.ID), false, nil)
		}
		seen[enc] = struct{}{}
		if len(seen) > cap_ {
			return fmt.Errorf("%w: %s", kidb.ErrUnsupported,
				i18n.T("ddl.unique_dedup_overflow", def.Name, idx.ID, cap_))
		}
	}
}

// DropIndex 删索引（sql.IndexAlterableTable）：Catalog 移除 → 桶清理
// （等值桶按行值回推 key；范围/字典序桶按 slot 直接 UNLINK）。
func (t *Table) DropIndex(ctx *sql.Context, indexName string) error {
	return sqlErr(t.dropIndex(ctx, indexName))
}

func (t *Table) dropIndex(ctx *sql.Context, indexName string) error {
	if err := RejectRO(ctx); err != nil {
		return err
	}
	deps := t.deps
	def, err := deps.Store.Load(ctx, t.def.Name)
	if err != nil {
		return err
	}
	if def == nil {
		return sql.ErrTableNotFound.New(t.def.Name)
	}
	idx := def.Index(indexName)
	if idx == nil {
		return fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.index_not_found", indexName, t.def.Name))
	}
	idxCopy := *idx
	var kept []meta.IndexDef
	for _, i := range def.Indexes {
		if i.ID != indexName {
			kept = append(kept, i)
		}
	}
	def.Indexes = kept
	if err := deps.Store.Save(ctx, def, def.Ver); err != nil {
		return err
	}
	deps.Cache.Invalidate()

	var cmds []kidb.Cmd
	flush := func() error {
		if len(cmds) == 0 {
			return nil
		}
		_, err := deps.Client.Pipeline(ctx, cmds)
		cmds = cmds[:0]
		return err
	}
	if idxCopy.Kind == meta.IndexRange {
		for slot := 0; slot < keycodec.NumSlots; slot++ {
			cmds = append(cmds, kidb.Cmd{Name: "UNLINK", Args: []any{keycodec.RangeBucketKey(def.Name, idxCopy.ID, uint16(slot), 0)}})
			if idxCopy.PrefixCopy {
				cmds = append(cmds, kidb.Cmd{Name: "UNLINK", Args: []any{keycodec.LexBucketKey(def.Name, idxCopy.ID, uint16(slot), 0)}})
			}
			if len(cmds) >= 512 {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return flush()
	}
	// 等值/唯一桶：按行值回推
	s := deps.Exec.Run(ctx, &exec.Request{Table: def, Kind: exec.FullScan})
	defer s.Close()
	colDef, _ := def.Column(idxCopy.Columns[0])
	for {
		row, err := s.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		pk := pkOf(def, row)
		ci := colIndexOf(def, idxCopy.Columns[0])
		if ci < 0 || row[ci] == nil {
			continue
		}
		enc, err := rowcodec.Encode(colDef.Type, row[ci])
		if err != nil {
			return err
		}
		slot := keycodec.Slot(keycodec.RowKey(def.Name, pk))
		cmds = append(cmds, kidb.Cmd{Name: "UNLINK", Args: []any{keycodec.EqBucketKey(def.Name, idxCopy.ID, enc, slot, 0)}})
		if idxCopy.PrefixCopy {
			cmds = append(cmds, kidb.Cmd{Name: "UNLINK", Args: []any{keycodec.LexBucketKey(def.Name, idxCopy.ID, slot, 0)}})
		}
		if len(cmds) >= 512 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// RenameIndex 不支持（超出文档化子集，docs/02 §2.4）。
func (t *Table) RenameIndex(ctx *sql.Context, fromIndexName, toIndexName string) error {
	return fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.rename_index_unsupported"))
}

func pkOf(def *meta.TableDef, row []any) string {
	for i, c := range def.Columns {
		if c.Name == def.PK {
			return fmt.Sprint(row[i])
		}
	}
	return ""
}

func colIndexOf(def *meta.TableDef, col string) int {
	for i, c := range def.Columns {
		if c.Name == col {
			return i
		}
	}
	return -1
}

// 编译期接口断言。
var (
	_ sql.TableCreator        = (*Database)(nil)
	_ sql.TableDropper        = (*Database)(nil)
	_ sql.IndexAlterableTable = (*Table)(nil)
)
