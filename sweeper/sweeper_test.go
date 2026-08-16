package sweeper

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"kidb/keycodec"
	"kidb/meta"
	"kidb/testutil"
	"kidb/txguard"
)

func sweepTable() *meta.TableDef {
	return &meta.TableDef{
		Name: "sess",
		Columns: []meta.ColumnDef{
			{Name: "uid", Type: meta.ColInt, NotNull: true},
			{Name: "city", Type: meta.ColString},
			{Name: "email", Type: meta.ColString},
		},
		PK: "uid",
		Indexes: []meta.IndexDef{
			{ID: "idx_city", Columns: []string{"city"}, Kind: meta.IndexEq},
			{ID: "uk_email", Columns: []string{"email"}, Kind: meta.IndexUnique},
		},
	}
}

// TestSweepExpired 到期行清扫：索引/登记册/计数/回执/预约全清（docs/07 §7.3）。
func TestSweepExpired(t *testing.T) {
	cli, reg, m := testutil.New(t)
	g := txguard.New(cli, reg, nil)
	now := time.Now()
	clock := func() time.Time { return now } // 共享钟：写入/清扫同源
	g.SetClock(clock)
	tbl := sweepTable()
	ctx := context.Background()
	p := testutil.NewProbe(t, cli)

	_, err := g.WriteRow(ctx, txguard.WriteReq{
		Table: tbl, PK: "1",
		Fields: map[string]string{"city": "shanghai", "email": "a@x.com"},
		TTL:    time.Hour,
	})
	require.NoError(t, err)

	m.FastForward(time.Hour + time.Second) // 行物理过期（miniredis TTL 钟）
	now = now.Add(time.Hour + time.Second) // 共享钟同步推进（回执 grace 内）
	require.False(t, p.Exists(keycodec.RowKey(tbl.Name, "1")))

	sw := New(cli, reg)
	sw.SetClock(clock)
	slot := keycodec.Slot(keycodec.RowKey(tbl.Name, "1"))
	n, err := sw.SweepSlot(ctx, tbl, slot)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// 全清断言（docs/12 §12.2 清扫不变式）
	require.Empty(t, p.ZScore(keycodec.EqBucketKey(tbl.Name, "idx_city", "shanghai", slot, 0), "1"), "索引桶")
	require.Empty(t, p.ZScore(keycodec.ExpKeyN(tbl.Name, slot, keycodec.ExpShardFor("1", 1), 1), "1"), "登记册")
	require.Equal(t, "0", p.Get(keycodec.CntKey(tbl.Name, slot)), "计数")
	require.False(t, p.Exists(keycodec.ReceiptKey(tbl.Name, "1")), "回执")
	require.False(t, p.Exists(keycodec.UniqueKey(tbl.Name, "uk_email", "a@x.com")), "唯一预约")

	// 幂等：再扫为零
	n, err = sw.SweepSlot(ctx, tbl, slot)
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

// TestSweepSkipsResurrected 复活复查不变式（write_row.lua 头部跨脚本约定）：
// 行已复活（exp score 在未来）时 sweeper 必须跳过，绝不清活行的索引。
func TestSweepSkipsResurrected(t *testing.T) {
	cli, reg, m := testutil.New(t)
	g := txguard.New(cli, reg, nil)
	now := time.Now()
	clock := func() time.Time { return now }
	g.SetClock(clock)
	tbl := sweepTable()
	ctx := context.Background()
	p := testutil.NewProbe(t, cli)

	_, err := g.WriteRow(ctx, txguard.WriteReq{
		Table: tbl, PK: "2",
		Fields: map[string]string{"city": "beijing", "email": "b@x.com"},
		TTL:    time.Hour,
	})
	require.NoError(t, err)

	// 复活：到期前 1s 重写为新 TTL（exp score 指向未来）
	m.FastForward(time.Hour - time.Second)
	now = now.Add(time.Hour - time.Second)
	_, err = g.WriteRow(ctx, txguard.WriteReq{
		Table: tbl, PK: "2",
		Fields: map[string]string{"city": "beijing", "email": "b@x.com"},
		TTL:    time.Hour,
	})
	require.NoError(t, err)
	m.FastForward(2 * time.Second) // 越过旧 score、未到新 score
	now = now.Add(2 * time.Second)

	sw := New(cli, reg)
	sw.SetClock(clock)
	slot := keycodec.Slot(keycodec.RowKey(tbl.Name, "2"))
	n, err := sw.SweepSlot(ctx, tbl, slot)
	require.NoError(t, err)
	require.Equal(t, 0, n, "复活行必须被复查跳过")

	require.True(t, p.Exists(keycodec.RowKey(tbl.Name, "2")))
	require.Equal(t, "0", p.ZScore(keycodec.EqBucketKey(tbl.Name, "idx_city", "beijing", slot, 0), "2"))
	require.Equal(t, "1", p.Get(keycodec.CntKey(tbl.Name, slot)))
}

func sprint(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(v)
	}
}
