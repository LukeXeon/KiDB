package controller

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"kidb/bucketmap"
	"kidb/exec"
	"kidb/internal/redistest"
	"kidb/keycodec"
	"kidb/meta"
	"kidb/txguard"
)

func splitTable() *meta.TableDef {
	return &meta.TableDef{
		Name: "hot",
		Columns: []meta.ColumnDef{
			{Name: "id", Type: meta.ColInt, NotNull: true},
			{Name: "city", Type: meta.ColString},
			{Name: "age", Type: meta.ColInt},
		},
		PK: "id",
		Indexes: []meta.IndexDef{
			{ID: "idx_city", Columns: []string{"city"}, Kind: meta.IndexEq},
			{ID: "idx_age", Columns: []string{"age"}, Kind: meta.IndexRange},
		},
	}
}

// sameSlotPKs 生成落在指定 slot 的 pk（测试用）。
func sameSlotPKs(table string, slot uint16, n int, rng *rand.Rand) []string {
	var out []string
	for len(out) < n {
		pk := strconv.Itoa(rng.Intn(1 << 30))
		if keycodec.Slot(keycodec.RowKey(table, pk)) == slot {
			out = append(out, pk)
		}
	}
	return out
}

// TestSplitEqUnderConcurrentWrites 核心不变式（docs/12 §12.3 P1 的单测形态）：
// 分裂进行中并发写入，任意时刻等值查询结果 == 模型（不多行、不少行）。
func TestSplitEqUnderConcurrentWrites(t *testing.T) {
	cli, reg, _ := redistest.New(t)
	bm := bucketmap.New(cli, reg)
	g := txguard.New(cli, reg, bm)
	sp := NewSplitter(cli, reg, bm)
	e := exec.New(cli, reg)
	e.SetBucketMap(bm)
	tbl := splitTable()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(7))

	slot := uint16(1234)
	pks := sameSlotPKs(tbl.Name, slot, 300, rng)

	// 种子：200 行 hot=shanghai
	var mu sync.Mutex
	alive := map[string]bool{}
	for _, pk := range pks[:200] {
		_, err := g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: pk, Fields: map[string]string{"city": "shanghai"}})
		require.NoError(t, err)
		alive[pk] = true
	}

	// 并发写入器（继续写/改/删）
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 200
		for {
			select {
			case <-stop:
				return
			default:
			}
			pk := pks[rng.Intn(len(pks))]
			mu.Lock()
			if rng.Intn(4) == 0 {
				_, _ = g.DeleteRow(ctx, tbl, pk)
				alive[pk] = false
			} else {
				for attempt := 0; attempt < 5; attempt++ {
					_, err := g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: pk, Fields: map[string]string{"city": "shanghai"}})
					if err == nil {
						alive[pk] = true
						break
					}
				}
			}
			i++
			mu.Unlock()
		}
	}()

	// 分裂（中途多次校验查询完整性）
	require.NoError(t, sp.SplitEq(ctx, tbl.Name, "idx_city", "shanghai", slot))
	close(stop)
	wg.Wait()

	// 终态校验：等值查询 == 模型
	eq := drainEq(t, e, tbl, "shanghai")
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, countAlive(alive), len(eq), "分裂后等值查询行数必须等于模型")
	for pk := range eq {
		require.True(t, alive[pk], "查询出了模型没有的行 %s", pk)
	}

	// 二次分裂（2→4 桶，验证续作/倍增路径）
	require.NoError(t, sp.SplitEq(ctx, tbl.Name, "idx_city", "shanghai", slot))
	eq = drainEq(t, e, tbl, "shanghai")
	require.Equal(t, countAlive(alive), len(eq), "二次分裂后仍须一致")
}

// TestSplitRangeCoversAll 范围桶分裂：查询覆盖区间无遗漏、无重复。
func TestSplitRangeCoversAll(t *testing.T) {
	cli, reg, _ := redistest.New(t)
	bm := bucketmap.New(cli, reg)
	g := txguard.New(cli, reg, bm)
	sp := NewSplitter(cli, reg, bm)
	e := exec.New(cli, reg)
	e.SetBucketMap(bm)
	tbl := splitTable()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(11))

	slot := uint16(77)
	pks := sameSlotPKs(tbl.Name, slot, 200, rng)
	for i, pk := range pks {
		_, err := g.WriteRow(ctx, txguard.WriteReq{
			Table: tbl, PK: pk,
			Fields: map[string]string{"city": "x", "age": strconv.Itoa(i)}, // age 0..199
		})
		require.NoError(t, err)
	}

	require.NoError(t, sp.SplitRange(ctx, tbl.Name, "idx_age", slot, 0))

	// 分裂后多个区间查询的完整性
	for _, q := range [][2]int{{0, 199}, {50, 149}, {0, 60}, {100, 199}} {
		rows := drainRange(t, e, tbl, q[0], q[1])
		seen := map[int]bool{}
		for _, r := range rows {
			age := int(r[2].(int64))
			require.False(t, seen[age], "区间查询出重复行")
			seen[age] = true
		}
		require.Equal(t, q[1]-q[0]+1, len(seen), "区间 [%d,%d] 覆盖不完整", q[0], q[1])
	}
}

func countAlive(m map[string]bool) int {
	n := 0
	for _, v := range m {
		if v {
			n++
		}
	}
	return n
}

func drainEq(t *testing.T, e *exec.Executor, tbl *meta.TableDef, val string) map[string]bool {
	t.Helper()
	idx := tbl.Index("idx_city")
	s := e.Run(context.Background(), &exec.Request{
		Table: tbl, Kind: exec.EqLookup, Index: idx, Values: []string{val},
		Pred: &exec.Predicate{Column: "city", Eq: []string{val}},
	})
	out := map[string]bool{}
	for {
		r, err := s.Next()
		if err != nil {
			break
		}
		out[fmt.Sprint(r[0])] = true
	}
	s.Close()
	return out
}

func drainRange(t *testing.T, e *exec.Executor, tbl *meta.TableDef, lo, hi int) [][]any {
	t.Helper()
	idx := tbl.Index("idx_age")
	rb := exec.RangeBound{Lo: float64(lo), Hi: float64(hi)}
	s := e.Run(context.Background(), &exec.Request{
		Table: tbl, Kind: exec.RangeLookup, Index: idx, Ranges: []exec.RangeBound{rb},
		Pred: &exec.Predicate{Column: "age", Ranges: []exec.RangeBound{rb}},
	})
	var out [][]any
	for {
		r, err := s.Next()
		if err != nil {
			break
		}
		out = append(out, r)
	}
	s.Close()
	return out
}

// TestMergeEqAfterSplit 合并镜像协议：分裂→合并→数据完整（docs/08 §8.3 末段）。
func TestMergeEqAfterSplit(t *testing.T) {
	cli, reg, _ := redistest.New(t)
	bm := bucketmap.New(cli, reg)
	g := txguard.New(cli, reg, bm)
	sp := NewSplitter(cli, reg, bm)
	e := exec.New(cli, reg)
	e.SetBucketMap(bm)
	tbl := splitTable()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(19))

	slot := uint16(555)
	pks := sameSlotPKs(tbl.Name, slot, 120, rng)
	for _, pk := range pks {
		_, err := g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: pk, Fields: map[string]string{"city": "hz"}})
		require.NoError(t, err)
	}

	require.NoError(t, sp.SplitEq(ctx, tbl.Name, "idx_city", "hz", slot))
	require.Equal(t, 120, len(drainEq(t, e, tbl, "hz")), "分裂后")

	// 合并回 1 桶
	require.NoError(t, sp.MergeEq(ctx, tbl.Name, "idx_city", "hz", slot))
	require.Equal(t, 120, len(drainEq(t, e, tbl, "hz")), "合并后行数不变")

	// 合并后再写仍正确（落到合并目标桶）
	_, err := g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: pks[0], Fields: map[string]string{"city": "hz"}})
	require.NoError(t, err)
	require.Equal(t, 120, len(drainEq(t, e, tbl, "hz")))

	// 再分裂仍正确（续作性）
	require.NoError(t, sp.SplitEq(ctx, tbl.Name, "idx_city", "hz", slot))
	require.Equal(t, 120, len(drainEq(t, e, tbl, "hz")), "再分裂后")
}

// TestMergeRangeAfterSplit 范围桶合并。
func TestMergeRangeAfterSplit(t *testing.T) {
	cli, reg, _ := redistest.New(t)
	bm := bucketmap.New(cli, reg)
	g := txguard.New(cli, reg, bm)
	sp := NewSplitter(cli, reg, bm)
	e := exec.New(cli, reg)
	e.SetBucketMap(bm)
	tbl := splitTable()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(23))

	slot := uint16(999)
	pks := sameSlotPKs(tbl.Name, slot, 100, rng)
	for i, pk := range pks {
		_, err := g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: pk, Fields: map[string]string{"age": strconv.Itoa(i)}})
		require.NoError(t, err)
	}

	require.NoError(t, sp.SplitRange(ctx, tbl.Name, "idx_age", slot, 0))
	require.Equal(t, 100, len(drainRange(t, e, tbl, 0, 99)), "分裂后全覆盖")

	// 找左子桶（idx 递增，左子桶 = Children[0]）
	sh, err := bm.LoadFresh(ctx, tbl.Name, "idx_age", slot)
	require.NoError(t, err)
	var leftIdx int
	for _, rb := range sh.Ranges {
		if rb.Hi != "+inf" {
			leftIdx = rb.Idx // [0, mid)
		}
	}
	require.NoError(t, sp.MergeRange(ctx, tbl.Name, "idx_age", slot, leftIdx))
	require.Equal(t, 100, len(drainRange(t, e, tbl, 0, 99)), "合并后全覆盖")
}
