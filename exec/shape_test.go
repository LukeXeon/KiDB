package exec

import (
	"context"
	"testing"

	"kidb/testutil"

	"github.com/stretchr/testify/require"
)

// TestCommandShape 命令形状不变式（回归钉死，docs/04 §4.1 翻译纪律，v7.0 新形状）：
// 点查单命令；**等值 = 每值 1+K 桶定位（默认 1）；范围 = 子桶数路（默认 1）；
// 全扫 = 1~n 册分页；COUNT(*) = 1 ZCOUNT**——16384 固定扇出消亡。
// 任何"多发命令"的意外回归（如扇出复活）都会在此现形。
func TestCommandShape(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	cc := testutil.NewCmdCounter(cli)
	ctx := context.Background()
	e := New(cc, reg)
	tbl := benchTable()

	shape := func(name string, req *Request, want map[string]int) {
		cc.Reset()
		s := e.Run(ctx, req)
		for {
			if _, err := s.Next(); err != nil {
				break
			}
		}
		s.Close()
		for cmd, n := range want {
			require.Equal(t, n, cc.Count(cmd), "%s 的 %s 命令数", name, cmd)
		}
	}

	shape("PointGet", &Request{Table: tbl, Kind: PointGet, Pks: []string{"1000"}},
		map[string]int{"HGETALL": 1})
	idxC := tbl.Index("idx_city")
	shape("EqLookup(单值默认桶)", &Request{Table: tbl, Kind: EqLookup, Index: idxC, Values: []string{"city7"}},
		map[string]int{"ZRANGE": 1})
	shape("EqLookup(三值)", &Request{Table: tbl, Kind: EqLookup, Index: idxC, Values: []string{"c1", "c2", "c3"}},
		map[string]int{"ZRANGE": 3})
	idxA := tbl.Index("idx_age")
	rng := RangeBound{Lo: 0, Hi: 100}
	shape("RangeLookup(默认单桶)", &Request{Table: tbl, Kind: RangeLookup, Index: idxA, Ranges: []RangeBound{rng}},
		map[string]int{"ZRANGEBYSCORE": 1})
	shape("Fullscan(单册)", &Request{Table: tbl, Kind: FullScan},
		map[string]int{"ZRANGE": 1})
	_, err := e.RowCount(ctx, tbl)
	require.NoError(t, err)
	require.Equal(t, 1, cc.Count("ZCOUNT"), "COUNT(*) = 集中册单 ZCOUNT（v7.0）")
}
