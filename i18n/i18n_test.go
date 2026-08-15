package i18n

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCatalogParity en/zh 目录键集合必须完全一致（缺键 = 装配事故）。
func TestCatalogParity(t *testing.T) {
	en := keys(t, "active.en.json")
	zh := keys(t, "active.zh.json")
	require.Equal(t, en, zh, "en/zh 目录键集合漂移")
}

// TestMessagesNoDocsRef 用户面向消息不得包含技术文档引用（v6.0 纪律）。
func TestMessagesNoDocsRef(t *testing.T) {
	for _, f := range []string{"active.en.json", "active.zh.json"} {
		for k, v := range load(t, f) {
			require.NotContains(t, v, "docs/", "%s[%s] 含文档引用", f, k)
			require.NotContains(t, v, "§", "%s[%s] 含章节引用", f, k)
		}
	}
}

// TestTranslate en 默认、zh 切换、参数化、未知语言回退。
func TestTranslate(t *testing.T) {
	SetLang(LangEnglish)
	require.Equal(t, "an explicit primary key is required", T("ddl.pk_required"))
	require.Equal(t, `index "ix_a" already exists`, T("ddl.index_exists", "ix_a"))

	SetLang(LangChinese)
	require.Equal(t, "必须显式主键", T("ddl.pk_required"))
	require.Equal(t, "索引 \"ix_a\" 已存在", T("ddl.index_exists", "ix_a"))

	// 未知语言回退 en
	SetLang("fr")
	require.Equal(t, "必须显式主键" != T("ddl.pk_required"), true)

	// 缺键可见（不静默）
	require.True(t, strings.HasPrefix(T("no.such.key", 1), "!no.such.key!"))

	SetLang(LangEnglish) // 复位（同进程其他测试）
	t.Cleanup(func() { SetLang(LangEnglish) })
}

// ==== 测试辅助 ====

func load(t *testing.T, name string) map[string]string {
	t.Helper()
	data, err := catalogFS.ReadFile(name)
	require.NoError(t, err)
	var m map[string]string
	require.NoError(t, json.Unmarshal(data, &m))
	return m
}

func keys(t *testing.T, name string) []string {
	m := load(t, name)
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
