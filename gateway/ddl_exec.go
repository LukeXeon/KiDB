package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"kidb"
	"kidb/ddl"
	"kidb/engine"
	"kidb/exec"
	"kidb/keycodec"
	"kidb/meta"
	"kidb/rowcodec"
)

// ExecDDL 执行 DDL 操作（docs/06 §6.3 作业流）。
// CREATE INDEX 走后台作业化（Building 标记 + JobRunner 巡检回填，docs/06 §6.3）；
// DROP TABLE/INDEX 的清理当前为同步执行（小表成立；大表清理走同样的作业化后续扩展）。
// 正确性论证：Catalog 先落库 → 并发写入即双写新索引；回填按当前行值 ZADD，
// ZSet 去重天然幂等；回填与删除交错的残留由回表校验兜底（docs/01 §1.7）。
func ExecDDL(ctx context.Context, op *ddl.Op, deps engine.Deps) error {
	switch op.Kind {
	case ddl.OpCreateTable:
		return createTable(ctx, op.Def, deps)
	case ddl.OpDropTable:
		return dropTable(ctx, op.Table, deps)
	case ddl.OpCreateIndex:
		return createIndex(ctx, op.Table, op.Index, deps)
	case ddl.OpDropIndex:
		return dropIndex(ctx, op.Table, op.IndexID, deps)
	}
	return fmt.Errorf("%w: 未知 DDL 操作 %v", kidb.ErrUnsupported, op.Kind)
}

// createTable 建表：Catalog 落库 + 表注册。
func createTable(ctx context.Context, def *meta.TableDef, deps engine.Deps) error {
	cur, err := deps.Store.Load(ctx, def.Name)
	if err != nil {
		return err
	}
	if cur != nil {
		return fmt.Errorf("%w: 表 %q 已存在", kidb.ErrUnsupported, def.Name)
	}
	if err := deps.Store.Save(ctx, def, 0); err != nil {
		return err
	}
	if err := deps.Store.RegisterTable(ctx, def.Name); err != nil {
		return err
	}
	deps.Cache.Invalidate()
	return nil
}

// dropTable 删表：同步清理数据（逐行 DeleteRow 保证索引/回执/登记册/预约全清）
// → Catalog 删除 + 注销注册表。
func dropTable(ctx context.Context, table string, deps engine.Deps) error {
	def, err := deps.Store.Load(ctx, table)
	if err != nil {
		return err
	}
	if def == nil {
		return fmt.Errorf("%w: 表 %q 不存在", kidb.ErrUnsupported, table)
	}
	// 数据清理：exp 登记册遍历逐行删除（有界批，docs/07 §7.4）
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
		// 删除走 txguard（索引/回执/计数/预约全清）
		if _, err := deps.Guard.DeleteRow(ctx, def, pkOf(def, row)); err != nil {
			return err
		}
	}
	// Catalog 删除 + 注销
	if _, err := deps.Client.Do(ctx, "DEL", keycodec.CatalogKey(table)); err != nil {
		return err
	}
	if err := deps.Store.UnregisterTable(ctx, table); err != nil {
		return err
	}
	if _, err := deps.Client.Do(ctx, "INCR", keycodec.SchemaVerKey()); err != nil {
		return err
	}
	deps.Cache.Invalidate()
	return nil
}

// createIndex 在线建索引（docs/06 §6.3 作业流）：
// 校验 → Catalog 落库（Building 标记，查询不可见）→ 作业落 `_job`（游标断点续作）
// → Controller 巡检回填 → 完成转可见。写入路径在 Building 期间即双写新索引
// （编辑器的 TableDef 含全部索引），回填与写入的交错由 ZSet 幂等吸收。
func createIndex(ctx context.Context, table string, idx *meta.IndexDef, deps engine.Deps) error {
	def, err := deps.Store.Load(ctx, table)
	if err != nil {
		return err
	}
	if def == nil {
		return fmt.Errorf("%w: 表 %q 不存在", kidb.ErrUnsupported, table)
	}
	if def.Index(idx.ID) != nil {
		return fmt.Errorf("%w: 索引 %q 已存在", kidb.ErrUnsupported, idx.ID)
	}
	if err := ddl.ValidateIndexForTable(idx, def); err != nil {
		return err
	}
	// 单 _job 槽位：表内有进行中作业则拒绝（DDL 低频管理面，队列化属过度设计）
	if job, err := deps.Store.GetJob(ctx, table); err != nil {
		return err
	} else if job != nil {
		return fmt.Errorf("%w: 表 %s 有进行中的 DDL 作业，完成后重试", kidb.ErrUnsupported, table)
	}
	idxCopy := *idx
	idxCopy.Building = true
	def.Indexes = append(def.Indexes, idxCopy)
	if err := deps.Store.Save(ctx, def, def.Ver); err != nil {
		return err
	}
	if err := deps.Store.SetJob(ctx, table, &meta.DDLJob{
		Type: "create_index", Index: &idxCopy, Cursor: 0, Started: time.Now().Unix(),
	}); err != nil {
		return err
	}
	deps.Cache.Invalidate()
	return nil
}

// dropIndex 删索引：Catalog 移除 → 桶清理（等值桶按行值回推 key；范围/字典序桶按 slot 直接 UNLINK）。
func dropIndex(ctx context.Context, table, indexID string, deps engine.Deps) error {
	def, err := deps.Store.Load(ctx, table)
	if err != nil {
		return err
	}
	if def == nil {
		return fmt.Errorf("%w: 表 %q 不存在", kidb.ErrUnsupported, table)
	}
	idx := def.Index(indexID)
	if idx == nil {
		return fmt.Errorf("%w: 索引 %q 不存在", kidb.ErrUnsupported, indexID)
	}
	idxCopy := *idx
	var kept []meta.IndexDef
	for _, i := range def.Indexes {
		if i.ID != indexID {
			kept = append(kept, i)
		}
	}
	def.Indexes = kept
	if err := deps.Store.Save(ctx, def, def.Ver); err != nil {
		return err
	}
	deps.Cache.Invalidate()

	// 桶清理（docs/06 §6.3：v1 同步）
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
			cmds = append(cmds, kidb.Cmd{Name: "UNLINK", Args: []any{keycodec.RangeBucketKey(table, idxCopy.ID, uint16(slot), 0)}})
			if idxCopy.PrefixCopy {
				cmds = append(cmds, kidb.Cmd{Name: "UNLINK", Args: []any{keycodec.LexBucketKey(table, idxCopy.ID, uint16(slot), 0)}})
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
			if isEOF(err) {
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
		cmds = append(cmds, kidb.Cmd{Name: "UNLINK", Args: []any{keycodec.EqBucketKey(table, idxCopy.ID, enc, slot, 0)}})
		if idxCopy.PrefixCopy {
			cmds = append(cmds, kidb.Cmd{Name: "UNLINK", Args: []any{keycodec.LexBucketKey(table, idxCopy.ID, slot, 0)}})
		}
		if len(cmds) >= 512 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
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

func isEOF(err error) bool { return errors.Is(err, io.EOF) }
