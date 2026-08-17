package indexer

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"kidb/keycodec"
	"kidb/meta"
	"kidb/metrics"
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
	// zcardAcross 汇总某值的桶成员数（v7.0：桶按值寻址，单桶）
	zcardAcross := func(val string) int {
		res, err := cli.Do(ctx, "ZCARD", keycodec.EqBucketKey(tbl.Name, idx.ID, val, 0))
		require.NoError(t, err)
		n, _ := strconv.Atoi(fmt.Sprint(res))
		return n
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
	for _, s := range slots {
		res, err := cli.Do(ctx, "LLEN", keycodec.AsyncLogKey(tbl.Name, idx.ID, s))
		require.NoError(t, err)
		require.Equal(t, "0", fmt.Sprint(res))
	}
}

// TestIndexDLQ 补写死信（v7.0 触发四③）：畸形条目不再静默丢弃——
// 落 dlq:idx:{table}:{idx} + index_dlq_total 指标，消费继续推进。
func TestIndexDLQ(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	m := metrics.New(prometheus.NewRegistry())
	tbl := asyncTable()
	ctx := context.Background()
	idx := tbl.Index("idx_tag")
	slot := keycodec.Slot(keycodec.RowKey(tbl.Name, "1"))

	// 塞一条畸形日志（字段数不对）
	_, err := cli.Do(ctx, "RPUSH", keycodec.AsyncLogKey(tbl.Name, idx.ID, slot), "bad-entry-no-fields")
	require.NoError(t, err)
	// 塞一条正常日志（经写入路径）
	g := txguard.New(cli, reg, nil)
	_, err = g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: "1", Fields: map[string]string{"tag": "go"}})
	require.NoError(t, err)

	ix := New(cli)
	ix.SetMetrics(m)
	n, err := ix.ConsumeLog(ctx, tbl, idx, slot)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	// 畸形条目落死信；正常条目已应用
	depth, err := cli.Do(ctx, "LLEN", keycodec.DLQKey(tbl.Name, idx.ID))
	require.NoError(t, err)
	require.Equal(t, "1", fmt.Sprint(depth), "畸形条目必须落死信队列")
	require.Equal(t, 1.0, promtest.ToFloat64(m.IndexDLQ), "index_dlq_total 接线")
	res, err := cli.Do(ctx, "ZCARD", keycodec.EqBucketKey(tbl.Name, idx.ID, "go", 0))
	require.NoError(t, err)
	require.Equal(t, "1", fmt.Sprint(res), "正常条目不受影响")
}
