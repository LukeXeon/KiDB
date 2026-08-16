package exec

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"kidb/meta"
	"kidb/testutil"
	"kidb/txguard"
)

// bench_test.go：查询/写入热路径的内核侧基准（docs/12 §12.7 门禁的种子）。
//
// 口径声明（诚实边界）：跑在 miniredis 单实例 + 本地回环上——测的是
// **内核 CPU/分配与命令形状**（16384 路种子扇出等结构成本可见），
// 不是真实集群的端到端 P99（那是 C 组性能门禁项，1 亿行/32 节点）。

func benchSetup(b *testing.B, rows int) (*Executor, context.Context) {
	b.Helper()
	cli, reg, _ := testutil.New(b)
	ctx := context.Background()
	tbl := &meta.TableDef{
		Name: "bench",
		Columns: []meta.ColumnDef{
			{Name: "uid", Type: meta.ColInt, TypeText: "bigint", NotNull: true},
			{Name: "city", Type: meta.ColString, TypeText: "varchar(32)"},
			{Name: "age", Type: meta.ColInt, TypeText: "int"},
		},
		PK: "uid",
		Indexes: []meta.IndexDef{
			{ID: "idx_city", Columns: []string{"city"}, Kind: meta.IndexEq},
			{ID: "idx_age", Columns: []string{"age"}, Kind: meta.IndexRange, Covering: []string{"city"}},
		},
	}
	g := txguard.New(cli, reg, nil)
	for i := 1; i <= rows; i++ {
		if _, err := g.WriteRow(ctx, txguard.WriteReq{
			Table:  tbl,
			PK:     strconv.Itoa(i),
			Fields: map[string]string{"city": fmt.Sprintf("city%d", i%50), "age": strconv.Itoa(20 + i%60)},
		}); err != nil {
			b.Fatal(err)
		}
	}
	return New(cli, reg), ctx
}

func drainBench(b *testing.B, s *RowStream) int {
	b.Helper()
	n := 0
	for {
		_, err := s.Next()
		if err != nil {
			break
		}
		n++
	}
	s.Close()
	return n
}

// BenchmarkWriteRow 写入热路径（write_row.lua 单 slot 原子提交 + 双索引维护）。
func BenchmarkWriteRow(b *testing.B) {
	cli, reg, _ := testutil.New(b)
	ctx := context.Background()
	tbl := &meta.TableDef{
		Name:    "benchw",
		Columns: []meta.ColumnDef{{Name: "uid", Type: meta.ColInt, TypeText: "bigint", NotNull: true}, {Name: "age", Type: meta.ColInt, TypeText: "int"}},
		PK:      "uid",
		Indexes: []meta.IndexDef{{ID: "idx_age", Columns: []string{"age"}, Kind: meta.IndexRange}},
	}
	g := txguard.New(cli, reg, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.WriteRow(ctx, txguard.WriteReq{
			Table: tbl, PK: strconv.Itoa(i),
			Fields: map[string]string{"age": strconv.Itoa(i % 100)},
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPointGetPK 主键点查（pipeline HGETALL 批量点查的单元素形态）。
func BenchmarkPointGetPK(b *testing.B) {
	e, ctx := benchSetup(b, 2000)
	req := &Request{Table: benchTable(), Kind: PointGet, Pks: []string{"1000"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		drainBench(b, e.Run(ctx, req))
	}
}

// BenchmarkEqLookup 等值索引（city=city7，50 值均匀分布，~40 命中）。
func BenchmarkEqLookup(b *testing.B) {
	e, ctx := benchSetup(b, 2000)
	tbl := benchTable()
	idx := tbl.Index("idx_city")
	req := &Request{Table: tbl, Kind: EqLookup, Index: idx, Values: []string{"city7"}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		drainBench(b, e.Run(ctx, req))
	}
}

// BenchmarkRangeTopK 范围 + ORDER BY + LIMIT 20（16384 路种子建堆的结构成本）。
func BenchmarkRangeTopK(b *testing.B) {
	e, ctx := benchSetup(b, 2000)
	tbl := benchTable()
	idx := tbl.Index("idx_age")
	rng := RangeBound{Lo: 0, Hi: 100}
	req := &Request{Table: tbl, Kind: RangeLookup, Index: idx, Ranges: []RangeBound{rng}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := e.Run(ctx, req)
		for j := 0; j < 20; j++ { // LIMIT 20 早停
			if _, err := s.Next(); err != nil {
				break
			}
		}
		s.Close()
	}
}

// BenchmarkCoveringRead 覆盖索引读（零回表：member 解码 + exp 活性校验）。
func BenchmarkCoveringRead(b *testing.B) {
	e, ctx := benchSetup(b, 2000)
	tbl := benchTable()
	idx := tbl.Index("idx_age")
	rng := RangeBound{Lo: 25, Hi: 45}
	req := &Request{
		Table: tbl, Kind: RangeLookup, Index: idx, Ranges: []RangeBound{rng},
		Pred:       &Predicate{Column: "age", Ranges: []RangeBound{rng}},
		Projection: []string{"uid", "age", "city"},
		Covering:   true,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		drainBench(b, e.Run(ctx, req))
	}
}

// BenchmarkFullscan 全表遍历（exp 登记册驱动，16384 册分页扇出的结构成本）。
func BenchmarkFullscan(b *testing.B) {
	e, ctx := benchSetup(b, 2000)
	req := &Request{Table: benchTable(), Kind: FullScan}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		drainBench(b, e.Run(ctx, req))
	}
}

func benchTable() *meta.TableDef {
	return &meta.TableDef{
		Name: "bench",
		Columns: []meta.ColumnDef{
			{Name: "uid", Type: meta.ColInt, TypeText: "bigint", NotNull: true},
			{Name: "city", Type: meta.ColString, TypeText: "varchar(32)"},
			{Name: "age", Type: meta.ColInt, TypeText: "int"},
		},
		PK: "uid",
		Indexes: []meta.IndexDef{
			{ID: "idx_city", Columns: []string{"city"}, Kind: meta.IndexEq},
			{ID: "idx_age", Columns: []string{"age"}, Kind: meta.IndexRange, Covering: []string{"city"}},
		},
	}
}
