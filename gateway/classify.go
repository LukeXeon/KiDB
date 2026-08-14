// Package gateway 是协议层组件集合：前置分类器、会话注册表、握手兼容。
// 本章对应文档 docs/02-SQL服务器.md。
package gateway

import (
	"strings"
	"unicode"
)

// Route 是前置分类器的路由判定（docs/02 §2.2）。
type Route int

const (
	// RouteEngine：DML 及其余语句 → go-mysql-server 引擎。
	RouteEngine Route = iota
	// RouteDDL：DDL 白名单 → KiDB DDL 路径（TiDB parser 解析，docs/02 §2.4）。
	RouteDDL
)

// Classify 决定语句的解析路径。判定集是封闭白名单：
// CREATE TABLE / CREATE [UNIQUE|FULLTEXT|SPATIAL] INDEX /
// DROP TABLE|INDEX / ALTER TABLE / TRUNCATE [TABLE]。
// 判不准的一律走引擎路径——宁可漏给引擎报错，不可错抢（docs/02 §2.2）。
func Classify(query string) Route {
	words := leadingWords(stripComments(query), 3)
	if len(words) == 0 {
		return RouteEngine
	}
	switch words[0] {
	case "CREATE":
		if len(words) >= 2 && words[1] == "TABLE" {
			return RouteDDL
		}
		if len(words) >= 2 && words[1] == "INDEX" {
			return RouteDDL
		}
		if len(words) >= 3 &&
			(words[1] == "UNIQUE" || words[1] == "FULLTEXT" || words[1] == "SPATIAL") &&
			words[2] == "INDEX" {
			return RouteDDL
		}
	case "DROP":
		if len(words) >= 2 && (words[1] == "TABLE" || words[1] == "INDEX") {
			return RouteDDL
		}
	case "ALTER":
		if len(words) >= 2 && words[1] == "TABLE" {
			return RouteDDL
		}
	case "TRUNCATE":
		// DDL 路径内明确报错 1235（docs/02 §2.4：无界操作不支持）。
		return RouteDDL
	}
	return RouteEngine
}

// leadingWords 取前 n 个"词"（标识符/关键字），大写返回。
// 字符串字面量与反引号标识符在 stripComments 阶段保留，
// 此处在词边界外遇到引号即停止（分类只看开头，引号出现说明首词已确定）。
func leadingWords(s string, n int) []string {
	var words []string
	i := 0
	for i < len(s) && len(words) < n {
		for i < len(s) && unicode.IsSpace(rune(s[i])) {
			i++
		}
		if i >= len(s) {
			break
		}
		if c := s[i]; c == '\'' || c == '"' || c == '`' {
			break
		}
		j := i
		for j < len(s) && !unicode.IsSpace(rune(s[j])) && s[j] != '(' && s[j] != ')' {
			j++
		}
		words = append(words, strings.ToUpper(s[i:j]))
		i = j
	}
	return words
}

// stripComments 移除 SQL 注释：`-- `（至行尾）、`#`（至行尾）、`/* */`，
// 对字符串字面量（'…'/"…"）与反引号标识符敏感——其内部的注释样字符不剥。
// 正确处理 MySQL 反斜杠转义（'it\'s'）。
func stripComments(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	for i < len(s) {
		c := s[i]
		// 字符串/标识符字面量原样穿过
		if c == '\'' || c == '"' || c == '`' {
			quote := c
			b.WriteByte(c)
			i++
			for i < len(s) {
				b.WriteByte(s[i])
				if s[i] == '\\' && quote != '`' && i+1 < len(s) {
					b.WriteByte(s[i+1])
					i += 2
					continue
				}
				if s[i] == quote {
					i++
					break
				}
				i++
			}
			continue
		}
		// 三类注释
		if c == '-' && i+2 < len(s) && s[i+1] == '-' && (s[i+2] == ' ' || s[i+2] == '\t') {
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		}
		if c == '#' {
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		}
		if c == '/' && i+1 < len(s) && s[i+1] == '*' {
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				return b.String() // 未闭合注释：余下全部视为注释
			}
			i += 2 + end + 2
			continue
		}
		b.WriteByte(c)
		i++
	}
	return b.String()
}
