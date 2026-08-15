package engine

import (
	"context"
	"io"
	"strconv"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"kidb/exec"
	"kidb/meta"
	"kidb/testutil"
	"kidb/txguard"
)

// TestProjectedIndexScan 投影 + 索引扫描的回归测试（B2 实证项）：
// gms coster（costed_index_scan.go）要求 PrimaryKeySchema().Schema 覆盖全部
// 索引列——投影窄 schema 曾让 PRIMARY 索引列缺失、nil stat 在 gms 内部 panic。
// 本测试钉死：投影查询走索引路径结果精确、行宽=投影宽。
func TestProjectedIndexScan(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	store := meta.NewCatalogStore(cli, reg)
	deps := Deps{
		Client: cli,
		Reg:    reg,
		Store:  store,
		Cache:  meta.NewCatalogCache(store),
		Exec:   exec.New(cli, reg),
		Guard:  txguard.New(cli, reg, nil),
	}
	def := &meta.TableDef{
		Name: "cov",
		Columns: []meta.ColumnDef{
			{Name: "uid", Type: meta.ColInt, TypeText: "bigint", NotNull: true},
			{Name: "city", Type: meta.ColString, TypeText: "varchar(32)", NotNull: true},
			{Name: "age", Type: meta.ColInt, TypeText: "int"},
			{Name: "note", Type: meta.ColString, TypeText: "varchar(255)"},
		},
		PK: "uid",
		Indexes: []meta.IndexDef{
			{ID: "idx_age", Columns: []string{"age"}, Kind: meta.IndexRange, Covering: []string{"city"}},
		},
	}
	require.NoError(t, store.Save(context.Background(), def, 0))
	g := txguard.New(cli, reg, nil)
	for i := 1; i <= 10; i++ {
		_, err := g.WriteRow(context.Background(), txguard.WriteReq{
			Table: def, PK: strconv.Itoa(i),
			Fields: map[string]string{"city": "c" + strconv.Itoa(i%3), "age": strconv.Itoa(20 + i), "note": "n" + strconv.Itoa(i)},
		})
		require.NoError(t, err)
	}

	eng, _, err := Build(deps)
	require.NoError(t, err)
	query := func(q string) (sql.Schema, [][]any) {
		t.Helper()
		sqlCtx := sql.NewContext(context.Background())
		sqlCtx.SetCurrentDatabase("kidb")
		sch, iter, _, err := eng.Query(sqlCtx, q)
		require.NoError(t, err, q)
		defer iter.Close(sqlCtx)
		var rows [][]any
		for {
			row, err := iter.Next(sqlCtx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			rows = append(rows, row)
		}
		return sch, rows
	}

	// 等值谓词 + 单列表投影（coster 必跑）：结果精确、行宽=1
	sch, rows := query("SELECT note FROM cov WHERE age = 25")
	require.Len(t, sch, 1)
	require.Len(t, rows, 1)
	require.Equal(t, "n5", rows[0][0])

	// 覆盖命中（city,age ⊆ 索引列+覆盖列+pk）：投影两列 + 有序 + LIMIT
	sch, rows = query("SELECT city, age FROM cov WHERE age >= 25 ORDER BY age DESC LIMIT 3")
	require.Len(t, sch, 2)
	require.Len(t, rows, 3)
	require.Equal(t, int64(30), rows[0][1]) // age 最大 30（i=10）
	require.Equal(t, int64(29), rows[1][1])

	// 零宽投影（COUNT 经索引路径）
	_, rows = query("SELECT COUNT(*) FROM cov WHERE age >= 25")
	require.Len(t, rows, 1)
	require.Equal(t, int64(6), rows[0][0]) // age 25..30 = 6 行
}
