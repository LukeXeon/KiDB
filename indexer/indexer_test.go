package indexer

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"kidb/keycodec"
	"kidb/meta"
	"kidb/testutil"
	"kidb/txguard"
)

func asyncTable() *meta.TableDef {
	return &meta.TableDef{
		Name: "feed",
		Columns: []meta.ColumnDef{
			{Name: "id", Type: meta.ColInt, NotNull: true},
			{Name: "tag", Type: meta.ColString},
		},
		PK: "id",
		Indexes: []meta.IndexDef{
			{ID: "idx_tag", Columns: []string{"tag"}, Kind: meta.IndexEq, Async: true},
		},
	}
}

// TestAsyncIndexFlow 异步索引全链路：写入只记日志 → Indexer 消费落桶 → 日志截断。
// 查询正确性由回表校验兜底（docs/05 §5.2：最终一致，不会出错行）。
// 断言对 slot 碰撞健壮（两行同 slot 时日志/桶合一）。
func TestAsyncIndexFlow(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	g := txguard.New(cli, reg, nil)
	tbl := asyncTable()
	ctx := context.Background()
	idx := tbl.Index("idx_tag")
	slots := []uint16{
		keycodec.Slot(keycodec.RowKey(tbl.Name, "1")),
		keycodec.Slot(keycodec.RowKey(tbl.Name, "2")),
	}
	// zcardAcross 跨两行 slot 汇总某值的桶成员数（等值查询真实形态）
	zcardAcross := func(val string) int {
		total := 0
		for _, s := range slots {
			res, err := cli.Do(ctx, "ZCARD", keycodec.EqBucketKey(tbl.Name, idx.ID, val, s, 0))
			require.NoError(t, err)
			n, _ := strconv.Atoi(fmt.Sprint(res))
			total += n
		}
		return total
	}
	consumeAll := func() int {
		total := 0
		for _, s := range slots {
			n, err := New(cli).ConsumeLog(ctx, tbl, idx, s)
			require.NoError(t, err)
			total += n
		}
		return total
	}

	// 写入：异步索引不碰桶，只追加日志
	_, err := g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: "1", Fields: map[string]string{"tag": "go"}})
	require.NoError(t, err)
	_, err = g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: "2", Fields: map[string]string{"tag": "go"}})
	require.NoError(t, err)

	require.Equal(t, 0, zcardAcross("go"), "消费前桶为空（异步窗口：新行短暂查不到，docs/05 §5.2）")
	require.Equal(t, 2, consumeAll(), "两条日志都被消费")
	require.Equal(t, 2, zcardAcross("go"), "消费后两行进桶")

	// 变更：1 换值 → 消费后旧桶撤、新桶建
	_, err = g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: "1", Fields: map[string]string{"tag": "rust"}})
	require.NoError(t, err)
	require.Equal(t, 1, consumeAll())
	require.Equal(t, 1, zcardAcross("go"), "旧值桶只剩 pk=2")
	require.Equal(t, 1, zcardAcross("rust"), "新值桶有 pk=1")

	// 删除：墓碑条目消费后旧桶空
	_, err = g.DeleteRow(ctx, tbl, "1")
	require.NoError(t, err)
	require.Equal(t, 1, consumeAll())
	require.Equal(t, 0, zcardAcross("rust"), "删除后新值桶空")

	// 日志已截断
	ix := New(cli)
	for _, s := range slots {
		n, err := ix.LogBacklog(ctx, tbl, idx, s)
		require.NoError(t, err)
		require.EqualValues(t, 0, n)
	}
}
