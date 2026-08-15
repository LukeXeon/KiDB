package exec

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"kidb/testutil"
	"kidb/txguard"
)

// TestPushdownParity 是 docs/12 §12.3 P4 的落地：
// 服务端下推 vs 客户端校验，同输入结果集必须相同。
func TestPushdownParity(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	g := txguard.New(cli, reg, nil)
	tbl := seedTable()
	ctx := context.Background()

	// 随机种子数据
	rng := rand.New(rand.NewSource(42))
	cities := []string{"shanghai", "beijing", "hangzhou", "shenzhen"}
	for i := 1; i <= 200; i++ {
		_, err := g.WriteRow(ctx, txguard.WriteReq{
			Table: tbl, PK: strconv.Itoa(i),
			Fields: map[string]string{
				"city": cities[rng.Intn(len(cities))],
				"age":  strconv.Itoa(18 + rng.Intn(60)),
			},
		})
		require.NoError(t, err)
	}

	e := New(cli, reg)
	idx := tbl.Index("idx_city")

	cases := []struct {
		name string
		pred *Predicate
	}{
		{"eq 单值", &Predicate{Column: "city", Eq: []string{"shanghai"}}},
		{"eq 多值", &Predicate{Column: "city", Eq: []string{"shanghai", "beijing"}}},
		{"range 闭区间", &Predicate{Column: "age", Ranges: []RangeBound{{Lo: 30, Hi: 45}}}},
		{"range 开区间", &Predicate{Column: "age", Ranges: []RangeBound{{Lo: 30, Hi: 45, LoOpen: true, HiOpen: true}}}},
		{"range 半开", &Predicate{Column: "age", Ranges: []RangeBound{{Lo: 20, Hi: 70, HiOpen: true}}}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var reqBase Request
			if len(c.pred.Eq) > 0 {
				reqBase = Request{Table: tbl, Kind: EqLookup, Index: idx, Values: c.pred.Eq, Pred: c.pred}
			} else {
				reqBase = Request{Table: tbl, Kind: RangeLookup, Index: tbl.Index("idx_age"), Ranges: c.pred.Ranges, Pred: c.pred}
			}
			// 客户端校验路径
			rowsClient := drain(t, e.Run(ctx, &reqBase))
			// 服务端下推路径
			reqPD := reqBase
			reqPD.Pushdown = true
			rowsPush := drain(t, e.Run(ctx, &reqPD))

			require.Equal(t, len(rowsClient), len(rowsPush), "两路径行数必须一致")
			// 集合比对（顺序不保证）
			set := map[string]bool{}
			for _, r := range rowsClient {
				set[strconv.FormatInt(r[0].(int64), 10)] = true
			}
			for _, r := range rowsPush {
				require.True(t, set[strconv.FormatInt(r[0].(int64), 10)], "下推多出客户端没有的行")
			}
		})
	}
}

// TestPushdownSkipsExpired 下推路径同样拦截过期行（空 Hash 服务端跳过）。
func TestPushdownSkipsExpired(t *testing.T) {
	cli, reg, m := testutil.New(t)
	g := txguard.New(cli, reg, nil)
	tbl := seedTable()
	ctx := context.Background()
	seedRows(t, g, tbl, 20)

	// 行 1 物理过期（桶里残留 member）
	_, err := cli.Do(ctx, "PEXPIRE", "d:users:{1}", 1)
	require.NoError(t, err)
	m.FastForward(time.Second)

	e := New(cli, reg)
	idx := tbl.Index("idx_city")
	pred := &Predicate{Column: "city", Eq: []string{"shanghai"}}
	rows := drain(t, e.Run(ctx, &Request{
		Table: tbl, Kind: EqLookup, Index: idx, Values: []string{"shanghai"},
		Pred: pred, Pushdown: true,
	}))
	for _, r := range rows {
		require.NotEqual(t, int64(1), r[0], "过期行必须被服务端下推的空 Hash 跳过拦截")
	}
}
