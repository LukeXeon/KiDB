package exec

import (
	"context"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"kidb/keycodec"
	"kidb/meta"
	"kidb/testutil"
	"kidb/txguard"
)

func seedTable() *meta.TableDef {
	return &meta.TableDef{
		Name: "users",
		Columns: []meta.ColumnDef{
			{Name: "uid", Type: meta.ColInt, NotNull: true},
			{Name: "city", Type: meta.ColString},
			{Name: "age", Type: meta.ColInt},
		},
		PK: "uid",
		Indexes: []meta.IndexDef{
			{ID: "idx_city", Columns: []string{"city"}, Kind: meta.IndexEq},
			{ID: "idx_age", Columns: []string{"age"}, Kind: meta.IndexRange},
		},
	}
}

func seedRows(t *testing.T, g *txguard.Guard, tbl *meta.TableDef, n int) {
	ctx := context.Background()
	for i := 1; i <= n; i++ {
		city := "shanghai"
		if i%2 == 0 {
			city = "beijing"
		}
		_, err := g.WriteRow(ctx, txguard.WriteReq{
			Table:  tbl,
			PK:     strconv.Itoa(i),
			Fields: map[string]string{"city": city, "age": strconv.Itoa(20 + i%50)},
		})
		require.NoError(t, err)
	}
}

func drain(t *testing.T, s *RowStream) [][]any {
	t.Helper()
	var out [][]any
	for {
		r, err := s.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		out = append(out, r)
	}
	require.NoError(t, s.Close())
	return out
}

func TestFullScanAndPointGet(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	g := txguard.New(cli, reg, nil)
	tbl := seedTable()
	seedRows(t, g, tbl, 100)
	e := New(cli, reg)
	ctx := context.Background()

	// 全表遍历（exp 登记册驱动）
	rows := drain(t, e.Run(ctx, &Request{Table: tbl, Kind: FullScan}))
	require.Len(t, rows, 100)

	// 主键点查 + IN
	rows = drain(t, e.Run(ctx, &Request{Table: tbl, Kind: PointGet, Pks: []string{"3", "7", "404"}}))
	require.Len(t, rows, 2)
	require.Equal(t, int64(3), rows[0][0])

	// 精确行数（exp 登记册 ZCOUNT 汇总）
	n, err := e.RowCount(ctx, tbl, time.Now().Unix())
	require.NoError(t, err)
	require.Equal(t, uint64(100), n)
}

func TestEqLookupWithValidation(t *testing.T) {
	cli, reg, m := testutil.New(t)
	g := txguard.New(cli, reg, nil)
	tbl := seedTable()
	seedRows(t, g, tbl, 50)
	e := New(cli, reg)
	ctx := context.Background()

	idx := tbl.Index("idx_city")
	req := &Request{
		Table: tbl, Kind: EqLookup, Index: idx, Values: []string{"shanghai"},
		Pred: &Predicate{Column: "city", Eq: []string{"shanghai"}},
	}
	rows := drain(t, e.Run(ctx, req))
	require.Len(t, rows, 25)
	for _, r := range rows {
		require.Equal(t, "shanghai", r[1])
	}

	// 校验拦截演练：桶里放脏 member（模拟异步索引残留/分裂中间态），
	// 回表校验必须拦截（docs/04 §4.3）。
	slot := keycodec.Slot(keycodec.RowKey(tbl.Name, "1"))
	dirty := keycodec.EqBucketKey(tbl.Name, "idx_city", "shanghai", slot, 0)
	// 行 1 实际是 shanghai；把行 2（beijing）塞进 shanghai 桶制造脏数据
	_, err := cli.Do(ctx, "ZADD", dirty, 0, "2")
	require.NoError(t, err)
	rows = drain(t, e.Run(ctx, req))
	require.Len(t, rows, 25, "脏 member 必须被回表校验拦截")

	// 行过期演练：行 1 物理过期后桶里残留 member → 空 Hash 跳过
	row1 := keycodec.RowKey(tbl.Name, "1")
	_, err = cli.Do(ctx, "PEXPIRE", row1, 1)
	require.NoError(t, err)
	m.FastForward(time.Second)
	rows = drain(t, e.Run(ctx, req))
	require.Len(t, rows, 24, "过期行必须被空 Hash 拦截")
}

func TestRangeLookup(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	g := txguard.New(cli, reg, nil)
	tbl := seedTable()
	seedRows(t, g, tbl, 50) // age = 20..69
	e := New(cli, reg)
	ctx := context.Background()

	idx := tbl.Index("idx_age")
	rng := RangeBound{Lo: 30, Hi: 40} // [30, 40]
	req := &Request{
		Table: tbl, Kind: RangeLookup, Index: idx, Ranges: []RangeBound{rng},
		Pred: &Predicate{Column: "age", Ranges: []RangeBound{rng}},
	}
	rows := drain(t, e.Run(ctx, req))
	require.NotEmpty(t, rows)
	for _, r := range rows {
		age := r[2].(int64)
		require.GreaterOrEqual(t, age, int64(30))
		require.LessOrEqual(t, age, int64(40))
	}
	// 验证谓词侧：age ∈ [30,40] 的种子行数 = 每值各 1（i%50 在 1..50 覆盖 20..69）
	// age=20+i%50，i∈[1,50] → 10..19 行... 直接以全扫对照
	all := drain(t, e.Run(ctx, &Request{Table: tbl, Kind: FullScan}))
	want := 0
	for _, r := range all {
		age := r[2].(int64)
		if age >= 30 && age <= 40 {
			want++
		}
	}
	require.Equal(t, want, len(rows), "索引路径与全扫必须一致（双路径比对）")
}
