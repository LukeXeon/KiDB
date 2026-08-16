package exec

import (
	"context"
	"testing"

	"kidb/keycodec"
	"kidb/testutil"

	"github.com/stretchr/testify/require"
)

// TestCommandShape 命令形状不变式（回归钉死，docs/04 §4.1 翻译纪律）：
// 点查单命令；索引/全扫路径固定扇出 = 16384（桶/登记册按 slot 全散布，
// 空桶零补页）；任何"少发命令"的意外回归（如扇出漏桶）都会在此现形。
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
	shape("EqLookup(空桶)", &Request{Table: tbl, Kind: EqLookup, Index: idxC, Values: []string{"city7"}},
		map[string]int{"ZRANGE": keycodec.NumSlots})
	idxA := tbl.Index("idx_age")
	rng := RangeBound{Lo: 0, Hi: 100}
	shape("RangeLookup(空桶种子)", &Request{Table: tbl, Kind: RangeLookup, Index: idxA, Ranges: []RangeBound{rng}},
		map[string]int{"ZRANGEBYSCORE": keycodec.NumSlots})
	shape("Fullscan(空册)", &Request{Table: tbl, Kind: FullScan},
		map[string]int{"ZRANGE": keycodec.NumSlots})
	_, err := e.RowCount(ctx, tbl)
	require.NoError(t, err)
	require.Equal(t, keycodec.NumSlots, cc.Count("ZCOUNT"), "COUNT(*) = Σ ZCOUNT 固定扇出")
}
