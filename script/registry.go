// Package script 是 Lua 资产注册器（docs/05 §5.7）：
// 全部 Lua 以独立 .lua 文件存放于包内 lua/ 目录，经 embed 嵌入二进制，
// 启动期解析头部元数据并做静态校验（fail-fast）。禁止在 Go 源码里拼接 Lua 字符串。
package script

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io/fs"
	"kidb/i18n"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"embed"
)

//go:embed lua
var luaFS embed.FS

// Script 对应一个 .lua 文件；SHA1 用于 EVALSHA。
type Script struct {
	Name       string // 来自头部元数据 @name
	Version    int    // @version
	SHA1       string // 源文本的 sha1（hex）
	Src        string
	Idempotent bool   // @idempotent
	KeysDesc   string // @keys_desc（KEYS[1] 必须是路由 key）
}

// Registry 是 name→Script 注册表。
type Registry struct {
	byName map[string]*Script
}

// Get 按名取脚本。
func (r *Registry) Get(name string) (*Script, bool) {
	s, ok := r.byName[name]
	return s, ok
}

// List 返回全部脚本名（排序后，供测试与诊断）。
func (r *Registry) List() []string {
	names := make([]string, 0, len(r.byName))
	for n := range r.byName {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// forbiddenCall 命中禁 SCAN/事务纪律的最后一道闸（docs/05 §5.7）：
// 禁止 redis.call/pcall 的首参数为 scan 家族 / keys / multi / exec / watch。
var forbiddenCall = regexp.MustCompile(
	`(?i)redis\.(call|pcall)\s*\(\s*['"](scan|sscan|hscan|zscan|keys|multi|exec|watch)`)

var headerField = regexp.MustCompile(`^--\s*@([a-z_]+)\s+(.+?)\s*$`)

// Load 在启动期执行（fail-fast）：
//  1. 遍历 embed.FS 解析全部 .lua 文件与头部元数据；
//  2. 静态校验：禁 scan/keys/multi/exec/watch 调用；@keys_desc 必须声明
//     KEYS[1] 为路由 key；
//  3. 计算 SHA1，建立 name→script 注册表。
func Load() (*Registry, error) {
	return loadFrom(luaFS)
}

func loadFrom(fsys embed.FS) (*Registry, error) {
	reg := &Registry{byName: make(map[string]*Script)}
	entries, err := fs.ReadDir(fsys, "lua")
	if err != nil {
		return nil, fmt.Errorf("script: read embed dir: %w", err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("script: no lua assets embedded")
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".lua") {
			continue
		}
		b, err := fs.ReadFile(fsys, "lua/"+e.Name())
		if err != nil {
			return nil, fmt.Errorf("script: read %s: %w", e.Name(), err)
		}
		s, err := parse(e.Name(), string(b))
		if err != nil {
			return nil, err
		}
		if _, dup := reg.byName[s.Name]; dup {
			return nil, fmt.Errorf("script: duplicate @name %q", s.Name)
		}
		reg.byName[s.Name] = s
	}
	return reg, nil
}

// parse 解析单个脚本的头部元数据并静态校验（独立函数便于负例单测）。
func parse(filename, src string) (*Script, error) {
	s := &Script{}
	seen := map[string]bool{}
	for _, line := range strings.Split(src, "\n") {
		m := headerField.FindStringSubmatch(line)
		if m == nil {
			if strings.HasPrefix(strings.TrimSpace(line), "--") || strings.TrimSpace(line) == "" {
				continue // 普通注释/空行
			}
			break // 首行非注释即头部结束
		}
		key, val := m[1], m[2]
		seen[key] = true
		switch key {
		case "name":
			s.Name = val
		case "version":
			v, err := strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("script %s: bad @version %q", filename, val)
			}
			s.Version = v
		case "idempotent":
			s.Idempotent = val == "true"
		case "keys_desc":
			s.KeysDesc = val
		}
	}
	for _, req := range []string{"name", "version", "keys_desc", "idempotent"} {
		if !seen[req] {
			return nil, fmt.Errorf("script %s: missing @%s header", filename, req)
		}
	}
	if !strings.Contains(s.KeysDesc, "KEYS[1]") {
		return nil, fmt.Errorf("script %s: @keys_desc must declare KEYS[1] as router key", filename)
	}
	if m := forbiddenCall.FindString(src); m != "" {
		return nil, fmt.Errorf("%s", i18n.T("script.forbidden_call", filename, m))
	}
	sum := sha1.Sum([]byte(src))
	s.SHA1 = hex.EncodeToString(sum[:])
	s.Src = src
	return s, nil
}
