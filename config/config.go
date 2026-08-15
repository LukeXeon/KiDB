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
var Vars = map[string]VarDef{
	"schema_lease_ms":              {"1000", rangeValidator(100, 10000)},
	"bucket_split_members":         {"50000", nonNegative},
	"bucket_split_bytes":           {"8388608", rangeValidator(0, 16<<20)}, // ≤16MB rebalance 红线
	"bucket_split_qps_ratio":       {"0.4", nil},
	"bucket_merge_members":         {"12500", nonNegative},
	"bucket_merge_sustain_periods": {"3", nonNegative},
	"hotkey_replica_max":           {"8", rangeValidator(1, 8)},
	"hotkey_refresh_interval":      {"1s", nil},
	"hotkey_source":                {"telemetry", enumValidator("telemetry", "events", "both")},
	"hotkey_row_cache":             {"false", boolValidator},
	"replica_read":                 {"false", boolValidator}, // L3 副本读（适配器能力位缺失时自动无效）
	"nearcache_ttl":                {"3s", nil},
	"nearcache_capacity":           {"10000", nonNegative},
	"sweeper_batch":                {"512", nonNegative},
	"sweeper_max_batches_per_tick": {"4", nonNegative},
	"receipt_grace_period":         {"300s", nil},
	"telemetry_sample_ratio":       {"0.015625", nil}, // 1/64
	"scatter_concurrency":          {"64", nonNegative},
	"scatter_row_batch":            {"512", nonNegative},
	"scatter_deadline_headroom":    {"0.8", nil},
	"retry_backoff_base_ms":        {"50", nonNegative},
	"retry_backoff_max_ms":         {"2000", nonNegative},
	"plan_cache_capacity":          {"1024", nonNegative},
	"async_log_capacity":           {"100000", nonNegative},
	"async_log_alert_ratio":        {"0.8", nil},
	"ddl_backfill_rate_limit":      {"10000", nonNegative},
	"query_allow_fullscan_tables":  {"", tableListValidator},
	"query_fullscan_rate_limit":    {"10", nonNegative},
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

func nonNegative(s string) error {
	n, err := strconv.ParseFloat(s, 64)
	if err != nil || n < 0 {
		return fmt.Errorf("需为非负数")
	}
	return nil
}

func rangeValidator(lo, hi float64) func(string) error {
	return func(s string) error {
		n, err := strconv.ParseFloat(s, 64)
		if err != nil || n < lo || n > hi {
			return fmt.Errorf("需在 [%v,%v]", lo, hi)
		}
		return nil
	}
}

func enumValidator(allowed ...string) func(string) error {
	return func(s string) error {
		for _, a := range allowed {
			if s == a {
				return nil
			}
		}
		return fmt.Errorf("需为 %v 之一", allowed)
	}
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
