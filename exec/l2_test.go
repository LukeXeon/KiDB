package exec

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"kidb/internal/redistest"
	"kidb/meta"
	"kidb/nearcache"
	"kidb/txguard"
)

// TestL2Singleflight L2 请求合并（docs/08 §8.4）：同指纹并发查询共享一次散取。
// 32 路并发冷查询同一热值，散取命令量必须 ≈ 1 次（而非 32 次）；
// 全部调用方结果一致（回表校验各自执行）。
func TestL2Singleflight(t *testing.T) {
	cli, reg, _ := redistest.New(t)
	cc := newCmdCounter(cli)
	g := txguard.New(cli, reg, nil)
	tbl := &meta.TableDef{
		Name: "ev",
		Columns: []meta.ColumnDef{
			{Name: "id", Type: meta.ColInt, NotNull: true},
			{Name: "tag", Type: meta.ColString},
			{Name: "v", Type: meta.ColInt},
		},
		PK: "id",
		Indexes: []meta.IndexDef{
			{ID: "idx_tag", Columns: []string{"tag"}, Kind: meta.IndexEq},
		},
	}
	ctx := context.Background()
	for i := 1; i <= 100; i++ {
		_, err := g.WriteRow(ctx, txguard.WriteReq{
			Table: tbl, PK: strconv.Itoa(i),
			Fields: map[string]string{"tag": "hot", "v": strconv.Itoa(i)},
		})
		require.NoError(t, err)
	}

	e := New(cc, reg)
	e.SetNearCache(nearcache.NewSharded[[]string](1000, 3*time.Second))
	idx := tbl.Index("idx_tag")
	req := func() *Request {
		return &Request{
			Table: tbl, Kind: EqLookup, Index: idx, Values: []string{"hot"},
			Pred: &Predicate{Column: "tag", Eq: []string{"hot"}},
		}
	}

	const fan = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([][]int64, fan)
	for w := 0; w < fan; w++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			<-start
			rows := drain(t, e.Run(ctx, req()))
			ids := make([]int64, 0, len(rows))
			for _, r := range rows {
				ids = append(ids, r[0].(int64))
			}
			results[k] = ids
		}(w)
	}
	zBefore := cc.count("ZRANGE")
	close(start)
	wg.Wait()

	zAfter := cc.count("ZRANGE")
	scatterVolume := zAfter - zBefore
	// 单次散取 = 16384 slot 各一页；L2 合并后 32 路并发总量应 ≈ 1 次（容忍竞态 2 次）
	require.LessOrEqual(t, scatterVolume, 2*16384,
		"L2 未合并：32 路并发产生 %d 次桶读取（未合并将是 %d）", scatterVolume, fan*16384)
	for k := range results {
		require.Len(t, results[k], 100, "调用方 %d 结果行数", k)
	}
}

// TestL2FollowersDuringRefill 已有 L1 缓存时并发直接命中（L1/L2 接力）。
func TestL2FollowersDuringRefill(t *testing.T) {
	cli, reg, _ := redistest.New(t)
	cc := newCmdCounter(cli)
	g := txguard.New(cli, reg, nil)
	tbl := &meta.TableDef{
		Name: "ev2",
		Columns: []meta.ColumnDef{
			{Name: "id", Type: meta.ColInt, NotNull: true},
			{Name: "tag", Type: meta.ColString},
		},
		PK: "id",
		Indexes: []meta.IndexDef{
			{ID: "idx_tag", Columns: []string{"tag"}, Kind: meta.IndexEq},
		},
	}
	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		_, err := g.WriteRow(ctx, txguard.WriteReq{
			Table: tbl, PK: strconv.Itoa(i),
			Fields: map[string]string{"tag": "x"},
		})
		require.NoError(t, err)
	}
	e := New(cc, reg)
	e.SetNearCache(nearcache.NewSharded[[]string](1000, 3*time.Second))
	idx := tbl.Index("idx_tag")
	req := func() *Request {
		return &Request{Table: tbl, Kind: EqLookup, Index: idx, Values: []string{"x"},
			Pred: &Predicate{Column: "tag", Eq: []string{"x"}}}
	}
	rows := drain(t, e.Run(ctx, req()))
	require.Len(t, rows, 10)
	zAfterFirst := cc.count("ZRANGE")
	// 第二次同指纹查询：L1 命中，零散取
	rows = drain(t, e.Run(ctx, req()))
	require.Len(t, rows, 10)
	require.Equal(t, zAfterFirst, cc.count("ZRANGE"), "L1 命中后零散取")
}
