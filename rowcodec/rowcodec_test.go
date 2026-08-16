package rowcodec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMemberCodecRoundTrip 覆盖列 member 的 msgp 编码往返 + 歧义免疫
// （含 "|" 与 "\x1f" 的 pk/值——v1 拼接编码的歧义场景，docs/03 §3.5；v7.0 版本戳）。
func TestMemberCodecRoundTrip(t *testing.T) {
	// 无覆盖列：pk\x1fver 编码，解析还原 pk 与 ver
	m0 := EncodeMember("pk|with|pipes", 7, nil)
	require.Equal(t, "pk|with|pipes", MemberPK(m0, false))
	v, ok := MemberVer(m0, false)
	require.True(t, ok)
	require.Equal(t, uint64(7), v)

	// 有覆盖列：含分隔符的值往返无歧义（[pk, ver, covers...]）
	pk := "we|ird\x1fpk"
	covers := []string{"a|b", "c\x1fd", "普通文本"}
	m := EncodeMember(pk, 42, covers)
	require.Equal(t, pk, MemberPK(m, true))
	require.Equal(t, covers, MemberCovers(m, true))
	v, ok = MemberVer(m, true)
	require.True(t, ok)
	require.Equal(t, uint64(42), v)

	// 无覆盖列的 member 用 hasCovering=true 解析 → 严格全量解析失败 → 回退原样
	require.Equal(t, "plainpk", MemberPK("plainpk", true))
}

// TestMemberPKNeverCorruptsRawPK 原始 pk 不被 msgp/版本段误判（回退纪律）。
func TestMemberPKNeverCorruptsRawPK(t *testing.T) {
	for _, pk := range []string{"1", "abc", "\x93\xa1a\xa1b", "d:t:{1}", "", "\x00\x01", "no-sep"} {
		// 无覆盖列路径：无版本尾缀的畸形 member 按原样（防御）
		require.Equal(t, pk, MemberPK(pk, false))
	}
	// pk 内含 \x1f 时取末段数字为 ver——PlainMember 往返必还原
	m := PlainMember("a\x1fb", 9)
	require.Equal(t, "a\x1fb", MemberPK(m, false))
	v, ok := MemberVer(m, false)
	require.True(t, ok)
	require.Equal(t, uint64(9), v)
	// LexMember 版本段在 pk 后：同 (值,pk) 不同 ver 相邻排序（去重前提）
	require.True(t, LexMember("v", "p", 6) < LexMember("v", "p", 7))
	require.Equal(t, "v\x00p\x1f6", LexMember("v", "p", 6))
}
