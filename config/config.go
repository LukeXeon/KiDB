// Package config 实现"配置即数据"（docs/10 §10.2）：
// 集群级配置存于 cfg:global Hash，SET GLOBAL 经 config_set.lua CAS 原子更新，
// 各实例经版本校验循环秒级传播。变量表与校验规则即本包注册表。
package config

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"kidb"
	"kidb/keycodec"
	"kidb/script"
)

// VarDef 是变量定义（默认值 + 校验器）。
type VarDef struct {
	Default  string
	Validate func(string) error
}

// Vars 是 KiDB 变量注册表（docs/10 §10.2 变量表）。
//
// 纪律（docs/01 §1.0 设计原点）：变量只承载**改变语义**的选择——
// 全扫放行（逃生门）、副本读、行级近缓存（一致性取舍开关）。
// 一切调优参数（阈值/批大小/退避/限速）是内置常量，不是变量——
// 配置面以个位数为纪律，运维心智对标 Redis 原生。
var Vars = map[string]VarDef{
	"query_allow_fullscan_tables": {"", tableListValidator},
	"replica_read":                {"false", boolValidator}, // L3 副本读（适配器能力位缺失时自动无效）
	"hotkey_row_cache":            {"false", boolValidator}, // 行级近缓存（陈旧窗口语义，docs/08 §8.4）
}

// Store 是配置读写契约（docs/10 §10.2 ConfigStore）。
type Store struct {
	cli   kidb.KvClient
	reg   *script.Registry
	actor string // 修改者标识（_audit）
	clock func() time.Time
}

// New 构造。
func New(cli kidb.KvClient, reg *script.Registry, actor string) *Store {
	return &Store{cli: cli, reg: reg, actor: actor, clock: time.Now}
}

// Get 读变量（未设置返回默认值）。
func (s *Store) Get(ctx context.Context, name string) (string, bool, error) {
	res, err := s.cli.Do(ctx, "HGET", keycodec.CfgGlobalKey(), name)
	if err != nil {
		return "", false, err
	}
	if res == nil {
		d, ok := Vars[name]
		if !ok {
			return "", false, nil
		}
		return d.Default, false, nil // false = 未显式设置
	}
	return fmt.Sprint(res), true, nil
}

// Set 写变量（config_set.lua CAS；重试 ≤3 次）。
func (s *Store) Set(ctx context.Context, name, value string) error {
	def, ok := Vars[name]
	if !ok {
		return fmt.Errorf("%w: 未知变量 %q", kidb.ErrUnsupported, name)
	}
	if def.Validate != nil {
		if err := def.Validate(value); err != nil {
			return fmt.Errorf("%w: 变量 %s 值 %q 非法: %v", kidb.ErrUnsupported, name, value, err)
		}
	}
	cs, _ := s.reg.Get("config_set")
	for attempt := 0; attempt < 3; attempt++ {
		ver, err := s.Version(ctx)
		if err != nil {
			return err
		}
		out, err := s.cli.Eval(ctx, cs, []string{keycodec.CfgGlobalKey()},
			name, value, strconv.FormatUint(ver, 10), s.actor, strconv.FormatInt(s.clock().Unix(), 10))
		if err != nil {
			return err
		}
		arr, _ := out.([]any)
		if len(arr) > 0 && fmt.Sprint(arr[0]) == "stale" {
			continue // 并发写冲突重试（docs/10 §10.2）
		}
		return nil
	}
	return fmt.Errorf("%w: SET GLOBAL %s 冲突重试耗尽", kidb.ErrStaleMetadata, name)
}

// Version 返回配置版本（传播循环比对锚点）。
func (s *Store) Version(ctx context.Context) (uint64, error) {
	res, err := s.cli.Do(ctx, "HGET", keycodec.CfgGlobalKey(), "_ver")
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return strconv.ParseUint(fmt.Sprint(res), 10, 64)
}

// All 返回全部变量当前值（SHOW GLOBAL VARIABLES 数据源）。
func (s *Store) All(ctx context.Context) (map[string]string, error) {
	res, err := s.cli.Do(ctx, "HGETALL", keycodec.CfgGlobalKey())
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for n, d := range Vars {
		out[n] = d.Default
	}
	switch v := res.(type) {
	case map[string]string:
		for k, val := range v {
			if !strings.HasPrefix(k, "_") {
				out[k] = val
			}
		}
	case []any:
		for i := 0; i+1 < len(v); i += 2 {
			k := fmt.Sprint(v[i])
			if !strings.HasPrefix(k, "_") {
				out[k] = fmt.Sprint(v[i+1])
			}
		}
	}
	return out, nil
}

func boolValidator(s string) error {
	if s == "true" || s == "false" {
		return nil
	}
	return fmt.Errorf("需为 true/false")
}

// tableListValidator 表白名单（表名存在性由调用方结合 Catalog 校验）。
func tableListValidator(s string) error {
	for _, part := range strings.Split(s, ",") {
		if strings.TrimSpace(part) == "" && s != "" {
			return fmt.Errorf("空表名")
		}
	}
	return nil
}
