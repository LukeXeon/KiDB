package exec

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"kidb/keycodec"
	"kidb/meta"
	"kidb/script"
	"kidb/testutil"
	"kidb/txguard"
)

func seedCoverTable() *meta.TableDef {
	return &meta.TableDef{
		Name: "users",
		Columns: []meta.ColumnDef{
			{Name: "uid", Type: meta.ColInt, NotNull: true},
			{Name: "city", Type: meta.ColString},
			{Name: "age", Type: meta.ColInt},
		},
		PK: "uid",
		Indexes: []meta.IndexDef{
			{ID: "idx_age", Columns: []string{"age"}, Kind: meta.IndexRange, Covering: []string{"city"}},
		},
	}
}

// TestCoveringReadPath 覆盖索引读路径（docs/03 §3.5）：
// 投影∪谓词 ⊆ {索引列,覆盖列,pk} → 零回表（无 HGETALL/HMGET），活性经 exp ZSCORE。
func TestCoveringReadPath(t *testing.T) {
	cli, reg, m := testutil.New(t)
	cc := testutil.NewCmdCounter(cli)
	g := txguard.New(cli, reg, nil)
	tbl := seedCoverTable()
	ctx := context.Background()

	// 共享可推进钟（miniredis TIME 不随 FastForward 走——全链路同钟纪律）
	now := time.Now()
	clock := func() time.Time { return now }
	g.SetClock(clock)

	for i := 1; i <= 40; i++ {
		_, err := g.WriteRow(ctx, txguard.WriteReq{
			Table:  tbl,
			PK:     strconv.Itoa(i),
			Fields: map[string]string{"city": fmt.Sprintf("city%d", i%5), "age": strconv.Itoa(20 + i)},
		})
		require.NoError(t, err)
	}
	// 一行短 TTL：FastForward 后物理过期、member 残留（Sweeper 未跑）——
	// 覆盖路径必须经 ZSCORE 活性校验拦截（绝不返回过期行）
	_, err := g.WriteRow(ctx, txguard.WriteReq{
		Table:  tbl,
		PK:     "999",
		Fields: map[string]string{"city": "ghost", "age": "66"},
		TTL:    time.Second,
	})
	require.NoError(t, err)

	e := New(cc, reg)
	e.SetClock(clock)
	idx := tbl.Index("idx_age")
	rng := RangeBound{Lo: 25, Hi: 70}
	req := &Request{
		Table: tbl, Kind: RangeLookup, Index: idx, Ranges: []RangeBound{rng},
		Pred:       &Predicate{Column: "age", Ranges: []RangeBound{rng}},
		Projection: []string{"uid", "age", "city"},
		Covering:   true,
	}

	before := cc.Count("HGETALL") + cc.Count("HMGET")
	rows := drain(t, e.Run(ctx, req))
	require.Equal(t, before, cc.Count("HGETALL")+cc.Count("HMGET"), "覆盖路径禁止回表")
	require.Positive(t, cc.Count("ZSCORE"), "覆盖路径必须做 exp 活性校验")

	// 结果：age 25..60 的种子行（36 行）+ ghost（age 66，尚未过期）= 37，
	// 按 score 全局有序（ghost 排最后）
	require.Len(t, rows, 37)
	for i := 0; i < 36; i++ {
		require.Equal(t, int64(25+i), rows[i][1]) // age 列（投影序 [uid, age, city]）
		require.Equal(t, fmt.Sprintf("city%d", rows[i][0].(int64)%5), rows[i][2])
	}
	require.Equal(t, "ghost", rows[36][2])

	// 过期演练：uid=999（age=66 ∈ [25,70]）物理过期后不得出现
	now = now.Add(2 * time.Second) // 推进共享钟（行 TTL 1s 已过期）
	m.FastForward(2 * time.Second) // 推进 miniredis TTL 钟（行 key 物理过期）
	rows = drain(t, e.Run(ctx, req))
	require.Len(t, rows, 36, "过期行必须被 ZSCORE 活性校验拦截")
	for _, r := range rows {
		require.NotEqual(t, "ghost", r[2])
	}
}

// TestProjectionFetch 投影下推（docs/04 §4.3）：非覆盖时回表用 HMGET 子集。
func TestProjectionFetch(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	cc := testutil.NewCmdCounter(cli)
	g := txguard.New(cli, reg, nil)
	// 4 列表：投影 [city] + 谓词 age → 取列 {city,age} ⊊ 非主键列 {city,age,note}
	tbl := &meta.TableDef{
		Name: "t4",
		Columns: []meta.ColumnDef{
			{Name: "uid", Type: meta.ColInt, NotNull: true},
			{Name: "city", Type: meta.ColString},
			{Name: "age", Type: meta.ColInt},
			{Name: "note", Type: meta.ColString},
		},
		PK: "uid",
		Indexes: []meta.IndexDef{
			{ID: "idx_age", Columns: []string{"age"}, Kind: meta.IndexRange},
		},
	}
	ctx := context.Background()
	for i := 1; i <= 20; i++ {
		_, err := g.WriteRow(ctx, txguard.WriteReq{
			Table:  tbl,
			PK:     strconv.Itoa(i),
			Fields: map[string]string{"city": fmt.Sprintf("city%d", i%3), "age": strconv.Itoa(30 + i), "note": "x"},
		})
		require.NoError(t, err)
	}

	e := New(cc, reg)
	idx := tbl.Index("idx_age")
	rng := RangeBound{Lo: 30, Hi: 60}
	req := &Request{
		Table: tbl, Kind: RangeLookup, Index: idx, Ranges: []RangeBound{rng},
		Pred:       &Predicate{Column: "age", Ranges: []RangeBound{rng}},
		Projection: []string{"city"},
	}
	hgetBefore := cc.Count("HGETALL")
	rows := drain(t, e.Run(ctx, req))
	require.Equal(t, hgetBefore, cc.Count("HGETALL"), "投影子集禁止 HGETALL")
	require.Positive(t, cc.Count("HMGET"), "投影子集必须 HMGET")
	require.Len(t, rows, 20)
	for _, r := range rows {
		require.Len(t, r, 1, "行宽=投影宽度")
		require.Contains(t, []string{"city0", "city1", "city2"}, r[0])
	}
}

// TestCoveringFallback 覆盖 member 解码失败（脏 member 防御）→ 回表兜底，结果不错。
func TestCoveringFallback(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	g := txguard.New(cli, reg, nil)
	tbl := seedCoverTable()
	ctx := context.Background()
	_, err := g.WriteRow(ctx, txguard.WriteReq{
		Table: tbl, PK: "1",
		Fields: map[string]string{"city": "shanghai", "age": "30"},
	})
	require.NoError(t, err)

	// 手工塞一个裸 member（无 msgp 覆盖数组）进桶——模拟格式损坏
	slot := keycodec.Slot(keycodec.RowKey(tbl.Name, "2"))
	bk := keycodec.RangeBucketKey(tbl.Name, "idx_age", slot, 0)
	_, err = cli.Do(ctx, "ZADD", bk, 42, "2") // pk=2 行不存在
	require.NoError(t, err)
	rk2 := keycodec.RowKey(tbl.Name, "2")
	_, err = cli.Do(ctx, "ZADD", keycodec.ExpKeyN(tbl.Name, slot, keycodec.ExpShardFor("2", 1), 1), 9999999999, "2")
	require.NoError(t, err)
	_, err = cli.Do(ctx, "HSET", rk2, "city", "beijing", "age", "42")
	require.NoError(t, err)

	e := New(cli, reg)
	idx := tbl.Index("idx_age")
	rng := RangeBound{Lo: 25, Hi: 50}
	req := &Request{
		Table: tbl, Kind: RangeLookup, Index: idx, Ranges: []RangeBound{rng},
		Pred:       &Predicate{Column: "age", Ranges: []RangeBound{rng}},
		Projection: []string{"uid", "age", "city"},
		Covering:   true,
	}
	rows := drain(t, e.Run(ctx, req))
	require.Len(t, rows, 2, "裸 member 回退回表后两行都在")
	require.Equal(t, int64(30), rows[0][1])
	require.Equal(t, int64(42), rows[1][1])
}

var _ = script.Registry{} // 防误删 import（reg 形参一致性）
