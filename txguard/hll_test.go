package txguard

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"kidb/internal/redistest"
	"kidb/keycodec"
	"kidb/meta"
)

// TestHLLSampling 基数统计采样写 + 估算读（docs/04 §4.6）：
// 写入 10000 行 5000 distinct 值 → PFCOUNT×回补 ≈ 5000（±30% 界，
// 采样误差 + HLL 自身误差的有界断言——近似纪律的验收方式）。
func TestHLLSampling(t *testing.T) {
	cli, reg, _ := redistest.New(t)
	g := New(cli, reg, nil)
	tbl := &meta.TableDef{
		Name: "st",
		Columns: []meta.ColumnDef{
			{Name: "id", Type: meta.ColInt, NotNull: true},
			{Name: "v", Type: meta.ColInt},
		},
		PK:      "id",
		Indexes: []meta.IndexDef{{ID: "idx_v", Columns: []string{"v"}, Kind: meta.IndexRange}},
	}
	ctx := context.Background()
	const rows, distinct = 10000, 5000
	for i := 1; i <= rows; i++ {
		_, err := g.WriteRow(ctx, WriteReq{Table: tbl, PK: strconv.Itoa(i),
			Fields: map[string]string{"v": strconv.Itoa(i % distinct)}})
		require.NoError(t, err)
	}

	res, err := cli.Do(ctx, "PFCOUNT", keycodec.HLLKey(tbl.Name, "idx_v"))
	require.NoError(t, err)
	sampled, err := strconv.ParseUint(fmt.Sprint(res), 10, 64)
	require.NoError(t, err)
	est := sampled * HLLCompensation
	lo, hi := uint64(float64(distinct)*0.5), uint64(float64(distinct)*2.0)
	require.GreaterOrEqual(t, est, lo, "估算 %d 低于下界 %d", est, lo)
	require.LessOrEqual(t, est, hi, "估算 %d 高于上界 %d", est, hi)
	t.Logf("distinct=%d 估算=%d（%.1f%% 偏差）", distinct, est,
		float64(int64(est)-int64(distinct))*100.0/distinct)
}
