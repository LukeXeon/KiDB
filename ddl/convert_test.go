package ddl

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"kidb/meta"
)

// TestTableFromSchema 建表转换（gms PrimaryKeySchema → TableDef）。
func TestTableFromSchema(t *testing.T) {
	mkcol := func(name string, typ sql.Type, pk, notNull bool) *sql.Column {
		return &sql.Column{Name: name, Type: typ, PrimaryKey: pk, Nullable: !notNull}
	}
	sch := sql.NewPrimaryKeySchema(sql.Schema{
		mkcol("uid", types.Int64, true, true),
		mkcol("token", types.LongText, false, true),
		mkcol("age", types.Int32, false, false),
		mkcol("profile", types.JSON, false, false),
	}, 0)

	def, err := TableFromSchema("sessions", sch, `kidb:{"default_ttl":86400}`)
	if err != nil {
		t.Fatalf("TableFromSchema: %v", err)
	}
	if def.PK != "uid" || def.DefaultTTL != 86400 {
		t.Fatalf("def = %+v", def)
	}
	if len(def.Columns) != 4 || def.Columns[3].Type != meta.ColJSON {
		t.Fatalf("columns = %+v", def.Columns)
	}
	if def.Columns[0].Type != meta.ColInt || def.Columns[1].Type != meta.ColString {
		t.Fatalf("types = %+v", def.Columns)
	}

	// 复合主键拒绝
	_, err = TableFromSchema("bad", sql.NewPrimaryKeySchema(sql.Schema{
		mkcol("a", types.Int64, true, true), mkcol("b", types.Int64, true, true),
	}, 0, 1), "")
	if err == nil {
		t.Fatal("复合主键必须拒绝")
	}

	// 未知 payload 字段拒绝（严格解析）
	_, err = TableFromSchema("t2", sql.NewPrimaryKeySchema(sql.Schema{mkcol("id", types.Int64, true, true)}, 0),
		`kidb:{"max_row_bytes":1024}`)
	if err == nil {
		t.Fatal("已移除的 payload 字段必须报错（严格解析）")
	}
}

// TestIndexFromDef 索引转换（含形态推导/覆盖校验/自动字典序副本）。
func TestIndexFromDef(t *testing.T) {
	def := &meta.TableDef{
		Name: "t",
		Columns: []meta.ColumnDef{
			{Name: "id", Type: meta.ColInt, NotNull: true},
			{Name: "tag", Type: meta.ColString, NotNull: true},
			{Name: "note", Type: meta.ColString},
		},
		PK: "id",
	}
	idx, err := IndexFromDef(sql.IndexDef{
		Name: "uk_tag", Columns: []sql.IndexColumn{{Name: "tag"}},
		Constraint: sql.IndexConstraint_Unique,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateIndexForTable(idx, def); err != nil {
		t.Fatal(err)
	}
	if idx.Kind != meta.IndexUnique || !idx.PrefixCopy {
		t.Fatalf("字符串唯一索引必须自动带字典序副本: %+v", idx)
	}

	// 覆盖列必须 NOT NULL
	idx2, err := IndexFromDef(sql.IndexDef{
		Name: "idx_tag2", Columns: []sql.IndexColumn{{Name: "tag"}},
		Comment: `kidb:{"covering":["note"]}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateIndexForTable(idx2, def); err == nil {
		t.Fatal("可空覆盖列必须拒绝（member 编码 NULL 不保真）")
	}

	// async + unique 互斥
	idx3, _ := IndexFromDef(sql.IndexDef{
		Name: "uk2", Columns: []sql.IndexColumn{{Name: "tag"}},
		Constraint: sql.IndexConstraint_Unique, Comment: `kidb:{"async":true}`,
	})
	if err := ValidateIndexForTable(idx3, def); err == nil {
		t.Fatal("async + unique 互斥校验缺失")
	}
}
