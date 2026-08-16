package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"kidb/exec"
	"kidb/keycodec"
	"kidb/meta"
	"kidb/testutil"
	"kidb/txguard"
)

// TestL4Lifecycle L4 全生命周期（review P0-2 回归）：
// 激活 → 副本服务读 → 副本过期（60s TTL）→ 读侧 EXISTS 判定回退源桶（行不消失）→
// 冷却 3 tick 自动回收（注册表摘除）。
func TestL4Lifecycle(t *testing.T) {
	cli, reg, m := testutil.New(t)
	store := meta.NewCatalogStore(cli, reg)
	guard := txguard.New(cli, reg, nil)
	ctx := context.Background()

	tbl := &meta.TableDef{
		Name: "lt",
		Columns: []meta.ColumnDef{
			{Name: "uid", Type: meta.ColInt, TypeText: "bigint", NotNull: true},
			{Name: "city", Type: meta.ColString, TypeText: "varchar(32)"},
		},
		PK:      "uid",
		Indexes: []meta.IndexDef{{ID: "idx_city", Columns: []string{"city"}, Kind: meta.IndexEq}},
	}
	require.NoError(t, store.Save(ctx, tbl, 0))
	require.NoError(t, store.RegisterTable(ctx, tbl.Name))
	for _, pk := range []string{"1", "2"} {
		_, err := guard.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: pk, Fields: map[string]string{"city": "hot"}})
		require.NoError(t, err)
	}

	src := keycodec.EqBucketKeyEsc("lt", "idx_city", "hot", 0)
	l4 := NewL4(cli, reg)
	require.NoError(t, l4.Activate(ctx, "lt", "idx_city", src, 2))

	ex := exec.New(cli, reg)
	ex.SetL4(l4)
	readHot := func() int {
		s := ex.Run(ctx, &exec.Request{Table: tbl, Kind: exec.EqLookup, Index: &tbl.Indexes[0], Values: []string{"hot"},
			Pred: &exec.Predicate{Column: "city", Eq: []string{"hot"}}})
		defer s.Close()
		n := 0
		for {
			_, err := s.Next()
			if err != nil {
				break
			}
			n++
		}
		return n
	}
	// 激活后读（副本 or 源桶，至少读全）
	require.GreaterOrEqual(t, readHot(), 1, "激活后可读")

	// 副本过期（61s > 60s TTL；无 Tick 续期）——读侧必须回退源桶，行不消失
	m.FastForward(61 * time.Second)
	require.GreaterOrEqual(t, readHot(), 1, "副本过期后回退源桶，行不得消失")

	// 冷却回收：st:{桶} 无采样（ops=0）→ 连续 3 Tick → Deactivate
	for i := 0; i < 4; i++ {
		require.NoError(t, l4.Tick(ctx, store))
	}
	res, err := cli.Do(ctx, "HGET", keycodec.BucketMapHotKey("lt", "idx_city"), l4Field(src))
	require.NoError(t, err)
	require.Nil(t, res, "冷却 3 tick 后注册表应摘除")
}

// TestL4TickRefresh 活跃桶 Tick 续期：有采样信号时副本不过期、注册保留。
func TestL4TickRefresh(t *testing.T) {
	cli, reg, m := testutil.New(t)
	store := meta.NewCatalogStore(cli, reg)
	guard := txguard.New(cli, reg, nil)
	ctx := context.Background()

	tbl := &meta.TableDef{
		Name:    "lr",
		Columns: []meta.ColumnDef{{Name: "uid", Type: meta.ColInt, TypeText: "bigint", NotNull: true}, {Name: "city", Type: meta.ColString, TypeText: "varchar(32)"}},
		PK:      "uid",
		Indexes: []meta.IndexDef{{ID: "idx_city", Columns: []string{"city"}, Kind: meta.IndexEq}},
	}
	require.NoError(t, store.Save(ctx, tbl, 0))
	require.NoError(t, store.RegisterTable(ctx, tbl.Name))
	_, err := guard.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: "1", Fields: map[string]string{"city": "hot"}})
	require.NoError(t, err)

	src := keycodec.EqBucketKeyEsc("lr", "idx_city", "hot", 0)
	l4 := NewL4(cli, reg)
	require.NoError(t, l4.Activate(ctx, "lr", "idx_city", src, 2))

	// 有采样信号（模拟遥测命中）→ Tick 刷新续期 → 副本在 FF 30s 后仍活
	_, err = cli.Do(ctx, "HINCRBY", "st:"+src, "ops", 5)
	require.NoError(t, err)
	require.NoError(t, l4.Tick(ctx, store))
	m.FastForward(30 * time.Second)
	res, err := cli.Do(ctx, "EXISTS", keycodec.ReplicaKey(src, 1))
	require.NoError(t, err)
	require.Equal(t, "1", fmt.Sprint(res), "活跃桶副本应被 Tick 续期")
}
