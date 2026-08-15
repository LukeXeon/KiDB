package exec

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"kidb/meta"
	"kidb/nearcache"
	"kidb/testutil"
	"kidb/txguard"
)

// TestRowCache 行级近缓存（docs/08 §8.4 覆盖边界，hotkey_row_cache）：
//   - 命中零 RTT（行 key 无 HGETALL）；
//   - 条目 TTL ≤ 行 PTTL：行过期后缓存同步失效（绝不返回过期行）；
//   - 更新/删除的陈旧窗口 ≤ 默认 TTL（文档化取舍，默认关闭的原因）。
func TestRowCache(t *testing.T) {
	cli, reg, m := testutil.New(t)
	cc := newCmdCounter(cli)
	g := txguard.New(cli, reg, nil)
	tbl := &meta.TableDef{
		Name: "rc",
		Columns: []meta.ColumnDef{
			{Name: "id", Type: meta.ColInt, NotNull: true},
			{Name: "v", Type: meta.ColString},
		},
		PK: "id",
	}
	ctx := context.Background()

	now := time.Now()
	g.SetClock(func() time.Time { return now })
	e := New(cc, reg)
	e.SetRowCache(nearcache.NewRowCache(1000, 3*time.Second))

	write := func(pk, v string, ttl time.Duration) {
		_, err := g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: pk, Fields: map[string]string{"v": v}, TTL: ttl})
		require.NoError(t, err)
	}
	get := func(pk string) [][]any {
		return drain(t, e.Run(ctx, &Request{Table: tbl, Kind: PointGet, Pks: []string{pk}}))
	}

	write("1", "a", 0)
	before := cc.count("HGETALL")
	rows := get("1")
	require.Len(t, rows, 1)
	require.Equal(t, "a", rows[0][1])
	mid := cc.count("HGETALL")
	require.Equal(t, before+1, mid, "冷读回表一次")
	rows = get("1")
	require.Len(t, rows, 1)
	require.Equal(t, mid, cc.count("HGETALL"), "热读命中缓存零回表")

	// 更新陈旧窗口：写入新值后立即读仍见旧值（文档化取舍；默认关闭的根因）
	write("1", "b", 0)
	rows = get("1")
	require.Equal(t, "a", rows[0][1], "陈旧窗口内返回缓存值（≤3s 文档化语义）")

	// 过期一致性：行 TTL 到期 → 条目同步死亡（TTL=min(默认, PTTL)）
	write("2", "x", 1200*time.Millisecond)
	rows = get("2")
	require.Len(t, rows, 1)
	now = now.Add(2 * time.Second)      // 推进共享钟（写入侧 TTL 计算）
	m.FastForward(2 * time.Second)      // 推进 miniredis TTL 钟（行物理过期）
	time.Sleep(1300 * time.Millisecond) // otter 条目真实时钟到期（PTTL 1.2s）
	rows = get("2")
	require.Empty(t, rows, "行过期后条目必须同步失效（绝不返回过期行）")
}
