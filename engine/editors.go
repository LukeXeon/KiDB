package engine

import (
	"errors"
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"kidb"
	"kidb/internal/tuning"
	"kidb/rowcodec"
	"kidb/txguard"
)

// editors.go：DML 写路径的 gms 编辑器实现（docs/05）。
// 每个编辑器方法 = 一次 txguard 单行写入（单 slot Lua 原子提交）。

// Inserter 返回行插入器。
func (t *Table) Inserter(ctx *sql.Context) sql.RowInserter {
	return &editor{t: t}
}

// Updater 返回行更新器。
func (t *Table) Updater(ctx *sql.Context) sql.RowUpdater {
	return &editor{t: t}
}

// Deleter 返回行删除器。
func (t *Table) Deleter(ctx *sql.Context) sql.RowDeleter {
	return &editor{t: t}
}

// Replacer 返回 REPLACE 写入器（UPSERT 语义：写入路径本就 upsert，docs/05 §5.4）。
func (t *Table) Replacer(ctx *sql.Context) sql.RowReplacer {
	return &editor{t: t}
}

// editor 是 Insert/Update/Delete/Replace 四用的行编辑器。
// applied 计数本语句已提交行数（StatementBegin 重置）——多行 DML 失败时
// 错误信息携带部分成功明细（docs/05 §5.5：无整体回滚）。
type editor struct {
	t       *Table
	applied int
}

// StatementBegin 重置语句内计数。
func (e *editor) StatementBegin(ctx *sql.Context) { e.applied = 0 }

// DiscardChanges no-op（单行已原子提交，无暂存变更可弃）。
func (e *editor) DiscardChanges(ctx *sql.Context, _ error) error { return nil }

// StatementComplete no-op。
func (e *editor) StatementComplete(ctx *sql.Context) error { return nil }

// Close 关闭。
func (e *editor) Close(*sql.Context) error { return nil }

// Insert 单行插入。语义分层（docs/05 §5.4/§5.5）：
//   - 活行已存在 → UniqueKeyError（携带既有行——gms 用它区分 plain INSERT 报错 /
//     IGNORE 抑制 / ODKU 走合并更新分支）；
//   - 行不存在或已过期 → 全新插入（过期行视为不存在，Lua 内回执分支清残留）。
func (e *editor) Insert(ctx *sql.Context, row sql.Row) error {
	return sqlErr(e.insert(ctx, row))
}

func (e *editor) insert(ctx *sql.Context, row sql.Row) error {
	if err := RejectRO(ctx); err != nil {
		return err
	}
	pk, fields, err := e.splitRow(row)
	if err != nil {
		return err
	}
	// AUTO_INCREMENT：pk 缺失/为零时取序列值（引擎一般已先走 GetNextAutoIncrementValue，
	// 此处兜底，docs/05 §5.4 空洞语义与 MySQL 一致）
	if (pk == "" || pk == "0") && e.t.def.AutoIncrColumn != "" {
		n, err := e.t.deps.Guard.NextAutoID(ctx, e.t.def.Name)
		if err != nil {
			return err
		}
		pk = fmt.Sprint(n)
	}
	// 主键判重（plain INSERT 语义）：预读既有行，活行冲突报错带 Existing
	// （upsert 语义走 Replacer，不经此路）
	if existing, err := e.readExisting(ctx, pk); err != nil {
		return err
	} else if existing != nil {
		return sql.NewUniqueKeyErr(pk, true, existing)
	}
	if err := e.checkRowSize(pk, fields); err != nil {
		return e.withProgress(err)
	}
	_, err = e.t.deps.Guard.WriteRow(ctx, txguard.WriteReq{
		Table:  e.t.def,
		PK:     pk,
		Fields: fields,
		TTL:    e.t.rowTTL(),
	})
	if err != nil {
		return e.withProgress(translateWriteErr(err))
	}
	e.applied++
	return nil
}

// Replace 同 Insert（upsert）。
func (e *editor) Replace(ctx *sql.Context, row sql.Row) error {
	return e.Insert(ctx, row)
}

// Update 单行更新：主键不变 → 写入新行（Lua 撤旧建新）；
// 主键变更 → 删旧行 + 写新行（两步非原子——无跨 slot 事务定位，docs/01 §1.2）。
func (e *editor) Update(ctx *sql.Context, old, new sql.Row) error {
	return sqlErr(e.update(ctx, old, new))
}

func (e *editor) update(ctx *sql.Context, old, new sql.Row) error {
	if err := RejectRO(ctx); err != nil {
		return err
	}
	oldPK, _, err := e.splitRow(old)
	if err != nil {
		return err
	}
	newPK, fields, err := e.splitRow(new)
	if err != nil {
		return err
	}
	if oldPK != newPK {
		if _, err := e.t.deps.Guard.DeleteRow(ctx, e.t.def, oldPK); err != nil {
			return err
		}
	}
	if err := e.checkRowSize(newPK, fields); err != nil {
		return e.withProgress(err)
	}
	_, err = e.t.deps.Guard.WriteRow(ctx, txguard.WriteReq{
		Table:  e.t.def,
		PK:     newPK,
		Fields: fields,
		TTL:    e.t.rowTTL(),
	})
	if err != nil {
		return e.withProgress(translateWriteErr(err))
	}
	e.applied++
	return nil
}

// Delete 单行删除（命中已过期行 = 0 rows affected，docs/05 §5.5）。
func (e *editor) Delete(ctx *sql.Context, row sql.Row) error {
	return sqlErr(e.delete(ctx, row))
}

func (e *editor) delete(ctx *sql.Context, row sql.Row) error {
	if err := RejectRO(ctx); err != nil {
		return err
	}
	pk, _, err := e.splitRow(row)
	if err != nil {
		return err
	}
	deleted, err := e.t.deps.Guard.DeleteRow(ctx, e.t.def, pk)
	if err != nil {
		return e.withProgress(translateWriteErr(err))
	}
	if deleted {
		e.applied++
	}
	return nil
}

// readExisting 预读既有行（判重用）：返回解码后的行，不存在/已过期返回 nil。
func (e *editor) readExisting(ctx *sql.Context, pk string) (sql.Row, error) {
	res, err := e.t.deps.Client.Do(ctx, "HGETALL", rowKeyOf(e.t.def.Name, pk))
	if err != nil {
		return nil, err
	}
	raw := map[string]string{}
	switch v := res.(type) {
	case map[string]string:
		raw = v
	case map[any]any:
		for k, val := range v {
			raw[fmt.Sprint(k)] = fmt.Sprint(val)
		}
	}
	if len(raw) == 0 {
		return nil, nil
	}
	return rowcodec.DecodeRow(e.t.def, pk, raw), nil
}

// rowKeyOf 行 key（keycodec 布局）。
func rowKeyOf(table, pk string) string { return "d:" + table + ":{" + pk + "}" }

// splitRow 把 gms 行拆为 pk 与编码字段（nil → NULL = 字段缺失）。
func (e *editor) splitRow(row sql.Row) (pk string, fields map[string]string, err error) {
	fields = map[string]string{}
	if len(row) != len(e.t.def.Columns) {
		return "", nil, fmt.Errorf("%w: 行宽度 %d 与列数 %d 不符", kidb.ErrContractViolation, len(row), len(e.t.def.Columns))
	}
	for i, col := range e.t.def.Columns {
		v := row[i]
		if col.Name == e.t.def.PK {
			if v == nil {
				continue // AUTO_INCREMENT 兜底在 Insert
			}
			pk, err = rowcodec.Encode(col.Type, v)
			if err != nil {
				return "", nil, err
			}
			continue
		}
		if v == nil {
			continue
		}
		enc, err := rowcodec.Encode(col.Type, v)
		if err != nil {
			return "", nil, err
		}
		fields[col.Name] = enc
	}
	return pk, fields, nil
}

// rowTTL 行级 TTL：v1 走表级 default_ttl（行内 _ttl 伪列通道见 docs/07 §7.1，
// 随会话/伪列支持落地）。
func (t *Table) rowTTL() time.Duration {
	if t.def.DefaultTTL > 0 {
		return time.Duration(t.def.DefaultTTL) * time.Second
	}
	return 0
}

// withProgress 给错误附部分成功明细（docs/05 §5.5）。
func (e *editor) withProgress(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w（本语句已提交 %d 行，无整体回滚——docs/05 §5.5）", err, e.applied)
}

// checkRowSize 单行体积防线（docs/03 §3.4：max_row_bytes，超限 ERR_ROW_TOO_LARGE）。
func (e *editor) checkRowSize(pk string, fields map[string]string) error {
	total := len(pk)
	for f, v := range fields {
		total += len(f) + len(v)
	}
	if lim := tuning.Get().Txguard.MaxRowBytes; total > lim {
		return fmt.Errorf("%w: %d bytes > max_row_bytes=%d", kidb.ErrRowTooLarge, total, lim)
	}
	return nil
}

// translateWriteErr 把内核错误翻译为 gms 可识别的形态
// （唯一冲突 → 主键冲突错误类，INSERT IGNORE 依赖它识别）。
func translateWriteErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, kidb.ErrDuplicateKey) {
		return sql.ErrPrimaryKeyViolation.New(err.Error())
	}
	return err
}
