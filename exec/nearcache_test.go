package exec

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"kidb/internal/redistest"
	"kidb/nearcache"
	"kidb/txguard"
)

// TestL1NearCache L1 集成：指纹命中跳过散取；TTL 内新行短暂不可见（文档化语义）；
// 值变更的行被回表校验拦截（docs/08 §8.4：绝不回错行）。
func TestL1NearCache(t *testing.T) {
	cli, reg, _ := redistest.New(t)
	g := txguard.New(cli, reg)
	tbl := seedTable()
	seedRows(t, g, tbl, 20) // 10 shanghai / 10 beijing

	nc, err := nearcache.New[string, []string](1000, 3*time.Second)
	require.NoError(t, err)
	defer nc.Close()

	e := New(cli, reg)
	e.SetNearCache(nc)
	ctx := context.Background()
	idx := tbl.Index("idx_city")
	pred := &Predicate{Column: "city", Eq: []string{"shanghai"}}

	// 首轮：散取填充
	rows := drain(t, e.Run(ctx, &Request{Table: tbl, Kind: EqLookup, Index: idx, Values: []string{"shanghai"}, Pred: pred}))
	require.Len(t, rows, 10)
	_, ok := nc.Get("users|idx_city|shanghai")
	require.True(t, ok, "完全排空后指纹应入缓存")

	// 缓存窗口内的变更：行 1 换值（缓存列表陈旧仍含它，校验必须拦）
	_, err = g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: "1", Fields: map[string]string{"city": "shenzhen", "age": "99"}})
	require.NoError(t, err)

	rows = drain(t, e.Run(ctx, &Request{Table: tbl, Kind: EqLookup, Index: idx, Values: []string{"shanghai"}, Pred: pred}))
	require.Len(t, rows, 9, "换值行必须被回表校验拦截（缓存列表陈旧不算错）")
	for _, r := range rows {
		require.Equal(t, "shanghai", r[1])
	}

	// TTL 过期后重新散取见到新值
	nc.Remove("users|idx_city|shanghai") // 模拟过期摘除
	rows = drain(t, e.Run(ctx, &Request{Table: tbl, Kind: EqLookup, Index: idx, Values: []string{"shanghai"}, Pred: pred}))
	require.Len(t, rows, 9)
	// shenzhen 桶可见行 1
	rows = drain(t, e.Run(ctx, &Request{Table: tbl, Kind: EqLookup, Index: idx, Values: []string{"shenzhen"}, Pred: &Predicate{Column: "city", Eq: []string{"shenzhen"}}}))
	require.Len(t, rows, 1)
	require.Equal(t, int64(1), rows[0][0])
}

// 保证收集路径在无缓存时不 panic 且功能不变
func TestEqLookupNoCache(t *testing.T) {
	cli, reg, _ := redistest.New(t)
	g := txguard.New(cli, reg)
	tbl := seedTable()
	seedRows(t, g, tbl, 10)
	e := New(cli, reg)
	idx := tbl.Index("idx_city")
	rows := drain(t, e.Run(context.Background(), &Request{
		Table: tbl, Kind: EqLookup, Index: idx, Values: []string{"beijing"},
		Pred: &Predicate{Column: "city", Eq: []string{"beijing"}},
	}))
	require.Len(t, rows, 5)
}
