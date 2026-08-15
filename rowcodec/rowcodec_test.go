package rowcodec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMemberCodecRoundTrip 覆盖列 member 的 msgp 编码往返 + 歧义免疫
// （含 "|" 与 "\x1f" 的 pk/值——v1 拼接编码的歧义场景，docs/03 §3.5）。
func TestMemberCodecRoundTrip(t *testing.T) {
	// 无覆盖列：原始 pk 直通
	require.Equal(t, "pk|with|pipes", EncodeMember("pk|with|pipes", nil))
	require.Equal(t, "pk|with|pipes", MemberPK("pk|with|pipes", false))

	// 有覆盖列：含分隔符的值往返无歧义
	pk := "we|ird\x1fpk"
	covers := []string{"a|b", "c\x1fd", "普通文本"}
	m := EncodeMember(pk, covers)
	require.Equal(t, pk, MemberPK(m, true))
	require.Equal(t, covers, MemberCovers(m, true))

	// 无覆盖列的 member 用 hasCovering=true 解析 → 严格全量解析失败 → 回退原样
	require.Equal(t, "plainpk", MemberPK("plainpk", true))
}

// TestMemberPKNeverCorruptsRawPK 原始 pk 不被 msgp 误判（回退纪律）。
func TestMemberPKNeverCorruptsRawPK(t *testing.T) {
	for _, pk := range []string{"1", "abc", "\x93\xa1a\xa1b", "d:t:{1}", "", "\x00\x01"} {
		// 无覆盖列路径恒等
		require.Equal(t, pk, MemberPK(pk, false))
	}
}
