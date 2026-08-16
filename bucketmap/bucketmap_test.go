package bucketmap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"kidb/keycodec"
	"kidb/testutil"
)

// TestShardRoundTrip bm 分片 msgp 编码往返（docs/03 §3.4 编码纪律）。
func TestShardRoundTrip(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	s := New(cli, reg)
	ctx := context.Background()

	key := keycodec.BucketMapSlotKey("t", "idx", 42)
	// 写入一个等值条目 + next
	if _, err := s.CAS(ctx, key, 0, "next", 5); err != nil {
		t.Fatal(err)
	}
	s.Invalidate()
	sh, err := s.LoadFresh(ctx, "t", "idx", 42)
	require.NoError(t, err)
	require.Equal(t, uint64(1), sh.Version)
	require.Equal(t, 5, sh.Next)

	entry := &EqEntry{Buckets: []int{1, 2}, Split: &SplitInfo{State: Splitting, Parents: []int{0}, Children: []int{1, 2}}}
	if _, err := s.CAS(ctx, key, sh.Version, "e:shanghai", entry); err != nil {
		t.Fatal(err)
	}
	s.Invalidate()
	sh, err = s.LoadFresh(ctx, "t", "idx", 42)
	require.NoError(t, err)
	got := sh.Eq["shanghai"]
	require.NotNil(t, got)
	require.Equal(t, []int{1, 2}, got.Buckets)
	require.Equal(t, Splitting, got.Split.State)
	require.Equal(t, []int{1, 2}, got.Split.Children)

	// 范围区间（含 inf 边界）
	ranges := []RangeBucket{
		{Idx: 1, Lo: "-inf", Hi: "50", State: Active},
		{Idx: 2, Lo: "50", Hi: "+inf", State: Active},
	}
	if _, err := s.CAS(ctx, key, sh.Version, "r", ranges); err != nil {
		t.Fatal(err)
	}
	s.Invalidate()
	sh, err = s.LoadFresh(ctx, "t", "idx", 42)
	require.NoError(t, err)
	require.Len(t, sh.Ranges, 2)
	require.Equal(t, "50", sh.Ranges[0].Hi)

	// 路由规则随编码往返后正确
	require.Equal(t, []int{1}, sh.WriteTargetsRange(30))
	require.Equal(t, []int{2}, sh.WriteTargetsRange(99))
	require.Equal(t, []int{1, 2}, sh.ReadBucketsRange(0, 100))

	// stale CAS 拒绝
	_, err = s.CAS(ctx, key, 0, "e:x", &EqEntry{Buckets: []int{0}})
	require.Error(t, err)
}
