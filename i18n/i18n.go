// Package i18n 是 KiDB 的国际化消息目录（v6.0，docs/10 §10.1）：
// 用户面向消息（SQL 错误/提示）全部走本目录——en 默认，zh 可选
// （进程级 --lang 开关）。消息文本不得包含技术文档引用（docs 引用只留
// 代码注释）。
//
// 用法：fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("ddl.pk_required"))
// 带参：i18n.T("ddl.index_exists", name)——目录消息为 printf 格式串。
package i18n

import (
	"embed"
	"encoding/json"
	"fmt"
	"sync/atomic"

	gi18n "github.com/nicksnyder/go-i18n/v2/i18n"
	"golang.org/x/text/language"
)

//go:embed active.*.json
var catalogFS embed.FS

var (
	bundle *gi18n.Bundle
	// current 当前语言本地izer（atomic.Value 存 *gi18n.Localizer）
	current atomic.Value
)

// LangEnglish / LangChinese 是支持的语言标识（--lang 取值）。
const (
	LangEnglish = "en"
	LangChinese = "zh"
)

func init() {
	bundle = gi18n.NewBundle(language.English)
	bundle.RegisterUnmarshalFunc("json", json.Unmarshal)
	for _, f := range []string{"active.en.json", "active.zh.json"} {
		if _, err := bundle.LoadMessageFileFS(catalogFS, f); err != nil {
			panic("i18n: 目录加载失败 " + f + ": " + err.Error())
		}
	}
	SetLang(LangEnglish)
}

// SetLang 切换进程级语言（未知语言回退 en）。
func SetLang(lang string) {
	switch lang {
	case LangChinese:
		current.Store(gi18n.NewLocalizer(bundle, LangChinese, LangEnglish))
	default:
		current.Store(gi18n.NewLocalizer(bundle, LangEnglish))
	}
}

// T 取消息并按 printf 规则格式化。消息缺失 = 目录装配事故——
// 返回可见的 "!id!" 形态（宁可见错误文本也不静默英文泄漏给中文用户，反之亦然）。
func T(id string, args ...any) string {
	l := current.Load().(*gi18n.Localizer)
	msg, err := l.Localize(&gi18n.LocalizeConfig{MessageID: id})
	if err != nil {
		return "!" + id + "!" + fmt.Sprint(args...)
	}
	if len(args) == 0 {
		return msg
	}
	return fmt.Sprintf(msg, args...)
}
