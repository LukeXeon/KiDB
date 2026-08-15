package exec

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"kidb/internal/redistest"
	"kidb/keycodec"
	"kidb/txguard"
)

// TestMinMaxWithDirtyMembers MIN/MAX 端点校验（docs/04 §4.5 + §4.3 回表校验纪律）：
// 过期行/改值残留的脏 member 必须被跳过，首个有效候选即答案。
func TestMinMaxWithDirtyMembers(t *testing.T) {
	cli, reg, m := redistest.New(t)
	g := txguard.New(cli, reg, nil)
	tbl := seedTable()
	ctx := context.Background()

	// 写入 age 10..50
	for i := 1; i <= 5; i++ {
		_, err := g.WriteRow(ctx, txguard.WriteReq{
			Table: tbl, PK: strconv.Itoa(i),
			Fields: map[string]string{"city": "x", "age": strconv.Itoa(i * 10)},
		})
		require.NoError(t, err)
	}
	e := New(cli, reg)
	idx := tbl.Index("idx_age")

	score, _, found, err := e.MinMax(ctx, tbl, idx, true)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 10.0, score)

	score, _, found, err = e.MinMax(ctx, tbl, idx, false)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 50.0, score)

	// 制造脏 member：行 1（age=10）物理过期，桶里残留
	_, err = cli.Do(ctx, "PEXPIRE", keycodec.RowKey(tbl.Name, "1"), 1)
	require.NoError(t, err)
	m.FastForward(2 * 1e9) // 过期生效（ms 精度内任意大快进）

	score, _, found, err = e.MinMax(ctx, tbl, idx, true)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 20.0, score, "过期行的端点候选必须被回表校验跳过")

	// 制造改值脏 member：行 2 改为 35，但桶里塞一个旧值 20 的假 member
	_, err = g.WriteRow(ctx, txguard.WriteReq{
		Table: tbl, PK: "2", Fields: map[string]string{"city": "x", "age": "35"},
	})
	require.NoError(t, err)
	slot := keycodec.Slot(keycodec.RowKey(tbl.Name, "2"))
	_, err = cli.Do(ctx, "ZADD", keycodec.RangeBucketKey(tbl.Name, idx.ID, slot, 0), 20, "2")
	require.NoError(t, err)

	score, _, found, err = e.MinMax(ctx, tbl, idx, true)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 30.0, score, "改值残留候选必须被跳过（行值 35 ≠ 候选 score 20）")
}
