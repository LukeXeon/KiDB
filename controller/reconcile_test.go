package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"kidb/bucketmap"
	"kidb/keycodec"
	"kidb/kv"
	"kidb/meta"
	"kidb/metrics"
	"kidb/rowcodec"
	"kidb/testutil"
	"kidb/txguard"
)

// reconcile_test.go：对账角色测试（docs/12 §12.8）——基线零漂移 + 三类注入漂移
// 分别命中指标；TTL 清扫暂态不误报。

func reconcileFixture(t *testing.T) (*Reconciler, *metrics.Metrics, *meta.TableDef, kv.Client, context.Context) {
	cli, reg, _ := testutil.New(t)
	m := metrics.New(prometheus.NewRegistry())
	store := meta.NewCatalogStore(cli, reg)
	bm := bucketmap.New(cli, reg)
	r := NewReconciler(cli, store, bm, m)

	tbl := &meta.TableDef{
		Name: "rc",
		Columns: []meta.ColumnDef{
			{Name: "uid", Type: meta.ColInt, TypeText: "bigint", NotNull: true},
			{Name: "city", Type: meta.ColString, TypeText: "varchar(32)"},
			{Name: "age", Type: meta.ColInt, TypeText: "int"},
			{Name: "email", Type: meta.ColString, TypeText: "varchar(64)"},
		},
		PK: "uid",
		Indexes: []meta.IndexDef{
			{ID: "idx_city", Columns: []string{"city"}, Kind: meta.IndexEq, PrefixCopy: true},
			{ID: "idx_age", Columns: []string{"age"}, Kind: meta.IndexRange},
			{ID: "uk_email", Columns: []string{"email"}, Kind: meta.IndexUnique},
		},
	}
	g := txguard.New(cli, reg, nil)
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		_, err := g.WriteRow(ctx, txguard.WriteReq{
			Table: tbl, PK: fmt.Sprint(i),
			Fields: map[string]string{"city": fmt.Sprintf("c%d", i%2), "age": fmt.Sprint(20 + i), "email": fmt.Sprintf("u%d@x.com", i)},
		})
		require.NoError(t, err)
	}
	return r, m, tbl, cli, ctx
}

func driftCount(m *metrics.Metrics, kind string) float64 {
	return promtest.ToFloat64(m.ReconcileDrift.WithLabelValues(kind))
}

// TestReconcileBaseline 基线：无注入时全链路零漂移。
func TestReconcileBaseline(t *testing.T) {
	r, m, tbl, _, ctx := reconcileFixture(t)
	for i := 1; i <= 5; i++ {
		require.NoError(t, r.ReconcilePage(ctx, tbl))
	}
	require.Equal(t, 0.0, driftCount(m, "index_member_missing"))
	require.Equal(t, 0.0, driftCount(m, "index_score_mismatch"))
	require.Equal(t, 0.0, driftCount(m, "index_member_orphan"))
	require.Equal(t, 0.0, driftCount(m, "uniq_reservation_residual"))
}

// TestReconcileForwardDrift 正向：删等值桶成员 → missing 漂移。
func TestReconcileForwardDrift(t *testing.T) {
	r, m, tbl, cli, ctx := reconcileFixture(t)
	_, err := cli.Do(ctx, "ZREM", keycodec.EqBucketKey(tbl.Name, "idx_city", "c1", 0), rowcodec.PlainMember("1", 1))
	require.NoError(t, err)
	require.NoError(t, r.ReconcilePage(ctx, tbl))
	require.Equal(t, 1.0, driftCount(m, "index_member_missing"))
}

// TestReconcileLexDrift 正向：字典序副本成员缺失 → missing 漂移。
func TestReconcileLexDrift(t *testing.T) {
	r, m, tbl, cli, ctx := reconcileFixture(t)
	_, err := cli.Do(ctx, "ZREM", keycodec.LexBucketKey(tbl.Name, "idx_city", 0), rowcodec.LexMember("c1", "1", 1))
	require.NoError(t, err)
	require.NoError(t, r.ReconcilePage(ctx, tbl))
	require.Equal(t, 1.0, driftCount(m, "index_member_missing"))
}

// TestReconcileScoreMismatch 正向：范围桶 score 被篡改 → score_mismatch 漂移。
func TestReconcileScoreMismatch(t *testing.T) {
	r, m, tbl, cli, ctx := reconcileFixture(t)
	_, err := cli.Do(ctx, "ZADD", keycodec.RangeBucketKey(tbl.Name, "idx_age", 0), 999, rowcodec.PlainMember("2", 1))
	require.NoError(t, err)
	require.NoError(t, r.ReconcilePage(ctx, tbl))
	require.Equal(t, 1.0, driftCount(m, "index_score_mismatch"))
}

// TestReconcileOrphan 反向：死且已清扫（无行无回执）行的桶成员残留 → orphan 漂移。
func TestReconcileOrphan(t *testing.T) {
	r, m, tbl, cli, ctx := reconcileFixture(t)
	_, err := cli.Do(ctx, "ZADD", keycodec.EqBucketKey(tbl.Name, "idx_city", "c1", 0), 0, "ghost")
	require.NoError(t, err)
	require.NoError(t, r.ReconcilePage(ctx, tbl))
	require.Equal(t, 1.0, driftCount(m, "index_member_orphan"))
}

// TestReconcileUniqResidual 唯一预约残留：预约 key 的占有者行已死 → residual 漂移
// （把活行 u4 的预约改写指向不存在的行 key——模拟事故现场）。
func TestReconcileUniqResidual(t *testing.T) {
	r, m, tbl, cli, ctx := reconcileFixture(t)
	_, err := cli.Do(ctx, "SET", keycodec.UniqueKey(tbl.Name, "uk_email", "u4@x.com"),
		keycodec.RowKey(tbl.Name, "ghost")+"|12345")
	require.NoError(t, err)
	require.NoError(t, r.ReconcilePage(ctx, tbl))
	require.Equal(t, 1.0, driftCount(m, "uniq_reservation_residual"))
}

// TestReconcileSweepLagNoFalsePositive TTL 清扫暂态不误报：
// 行物理过期、清扫未跑（回执仍在、桶成员仍在）→ 反向检查不算 orphan。
func TestReconcileSweepLagNoFalsePositive(t *testing.T) {
	cli, reg, m := testutil.New(t)
	mt := metrics.New(prometheus.NewRegistry())
	store := meta.NewCatalogStore(cli, reg)
	r := NewReconciler(cli, store, bucketmap.New(cli, reg), mt)

	tbl := &meta.TableDef{
		Name: "rc",
		Columns: []meta.ColumnDef{
			{Name: "uid", Type: meta.ColInt, TypeText: "bigint", NotNull: true},
			{Name: "city", Type: meta.ColString, TypeText: "varchar(32)"},
		},
		PK:      "uid",
		Indexes: []meta.IndexDef{{ID: "idx_city", Columns: []string{"city"}, Kind: meta.IndexEq}},
	}
	g := txguard.New(cli, reg, nil)
	ctx := context.Background()
	_, err := g.WriteRow(ctx, txguard.WriteReq{
		Table: tbl, PK: "77", Fields: map[string]string{"city": "c1"}, TTL: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	m.FastForward(time.Minute) // 行物理过期；Sweeper 未跑（桶成员/回执仍在）

	require.NoError(t, r.ReconcilePage(ctx, tbl))
	require.Equal(t, 0.0, driftCount(mt, "index_member_orphan"), "清扫暂态不得误报 orphan")
	require.Equal(t, 0.0, driftCount(mt, "index_member_missing"))
}
