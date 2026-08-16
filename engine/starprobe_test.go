package engine

import (
	"context"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"

	"kidb/exec"
	"kidb/meta"
	"kidb/testutil"
	"kidb/txguard"
)

// TestProbeStarProjection 探针：SELECT * / 显式列 的投影下推与行宽（_ttl 策略决策依据）。
func newProbeSess() sql.Session {
	s := sql.NewBaseSession()
	s.SetCurrentDatabase("kidb")
	return s
}

func TestProbeStarProjection(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	store := meta.NewCatalogStore(cli, reg)
	def := &meta.TableDef{
		Name: "probe",
		Columns: []meta.ColumnDef{
			{Name: "uid", Type: meta.ColInt, TypeText: "bigint", NotNull: true},
			{Name: "city", Type: meta.ColString, TypeText: "varchar(32)"},
		},
		PK: "uid",
	}
	g := txguard.New(cli, reg, nil)
	if _, err := g.WriteRow(context.Background(), txguard.WriteReq{Table: def, PK: "1", Fields: map[string]string{"city": "x"}}); err != nil {
		t.Fatal(err)
	}
	deps := Deps{
		Client: cli, Reg: reg, Store: store, Cache: meta.NewCatalogCache(store),
		Exec:  exec.New(cli, reg), Guard: g,
		FullscanGate: func(ctx *sql.Context, table string, rows uint64) error { return nil },
	}
	if err := store.Save(context.Background(), def, 0); err != nil {
		t.Fatal(err)
	}
	eng, _, err := Build(deps)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"SELECT * FROM probe", "SELECT uid FROM probe", "SELECT _ttl FROM probe", "SELECT city, _ttl FROM probe"} {
		sch, iter, _, err := eng.Query(sql.NewContext(context.Background(), sql.WithSession(newProbeSess())), q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		var rows []sql.Row
		for {
			r, err := iter.Next(sql.NewContext(context.Background()))
			if err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("%s iter: %v", q, err)
			}
			rows = append(rows, r)
		}
		iter.Close(sql.NewContext(context.Background()))
		t.Logf("%s → schema宽 %d, 行 %v", q, len(sch), rows)
	}
}
