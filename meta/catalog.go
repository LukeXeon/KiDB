package meta

import (
	"context"
	"fmt"
	"strconv"

	"kidb"
	"kidb/keycodec"
	"kidb/script"
)

// CatalogStore 是 Catalog 的读写存储（docs/06 §6.1：`c:table:{table}` Hash，
// 字段 def=编码后的 TableDef、_ver=表级版本、_job=进行中的 DDL 作业）。
//
// def/_job 编码为 msgp 代码生成版（docs/03 §3.4；_fmtv 版本号内嵌，演进走 docs/06 §6.4）。
// TODO(impl): Save 的 CAS 当前为读-改-写，实现期改 config_set 风格 Lua 原子 CAS。
type CatalogStore struct {
	cli kidb.KvClient
	reg *script.Registry
}

// NewCatalogStore 构造存储（reg 供 catalog_set.lua 原子 CAS）。
func NewCatalogStore(cli kidb.KvClient, reg *script.Registry) *CatalogStore {
	return &CatalogStore{cli: cli, reg: reg}
}

// Load 读取表定义；表不存在返回 (nil, 0, nil)。
func (s *CatalogStore) Load(ctx context.Context, table string) (*TableDef, error) {
	res, err := s.cli.Do(ctx, "HGETALL", keycodec.CatalogKey(table))
	if err != nil {
		return nil, err
	}
	fields, err := asStringMap(res)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, nil
	}
	raw, ok := fields["def"]
	if !ok {
		return nil, fmt.Errorf("catalog %s: missing def field", table)
	}
	var def TableDef
	if _, err := def.UnmarshalMsg([]byte(raw)); err != nil {
		return nil, fmt.Errorf("catalog %s: decode def: %w", table, err)
	}
	ver, _ := strconv.ParseUint(fields["_ver"], 10, 64)
	def.Ver = ver
	return &def, nil
}

// Save 写表定义（catalog_set.lua 原子 CAS：HGET _ver 校验 → HSET def → _ver+1）。
// 并发 DDL 不丢更新；期望版本不符即 stale 报错（**不自动换基线重试**——
// 调用方拿着旧 def，换基线重试会覆盖并发变更，正是要防的丢更新）。
func (s *CatalogStore) Save(ctx context.Context, def *TableDef, expectVer uint64) error {
	raw, err := def.MarshalMsg(nil)
	if err != nil {
		return fmt.Errorf("catalog %s: encode def: %w", def.Name, err)
	}
	cs, _ := s.reg.Get("catalog_set")
	key := keycodec.CatalogKey(def.Name)
	out, err := s.cli.Eval(ctx, cs, []string{key}, string(raw), strconv.FormatUint(expectVer, 10), 2)
	if err != nil {
		return err
	}
	arr, _ := out.([]any)
	if len(arr) == 0 {
		return fmt.Errorf("catalog %s: bad cas reply %v", def.Name, out)
	}
	switch fmt.Sprint(arr[0]) {
	case "ok":
		// 全局 schema 版本递增（docs/06 §6.2：plan cache 与 lease 的失效锚点）。
		_, err = s.cli.Do(ctx, "INCR", keycodec.SchemaVerKey())
		return err
	case "stale":
		return fmt.Errorf("%w: catalog %s expect _ver=%d got %s", kidb.ErrStaleMetadata, def.Name, expectVer, fmt.Sprint(arr[1]))
	}
	return fmt.Errorf("catalog %s: unknown cas status %v", def.Name, arr[0])
}

// SetJob 写入 DDL 作业（Catalog `_job` 字段，docs/06 §6.3）。
func (s *CatalogStore) SetJob(ctx context.Context, table string, job *DDLJob) error {
	raw, err := job.MarshalMsg(nil)
	if err != nil {
		return err
	}
	_, err = s.cli.Do(ctx, "HSET", keycodec.CatalogKey(table), "_job", string(raw))
	return err
}

// GetJob 读 DDL 作业（无则 nil）。
func (s *CatalogStore) GetJob(ctx context.Context, table string) (*DDLJob, error) {
	res, err := s.cli.Do(ctx, "HGET", keycodec.CatalogKey(table), "_job")
	if err != nil || res == nil {
		return nil, err
	}
	var job DDLJob
	if _, err := job.UnmarshalMsg([]byte(fmt.Sprint(res))); err != nil {
		return nil, err
	}
	return &job, nil
}

// ClearJob 清除完成的作业。
func (s *CatalogStore) ClearJob(ctx context.Context, table string) error {
	_, err := s.cli.Do(ctx, "HDEL", keycodec.CatalogKey(table), "_job")
	return err
}

// SchemaVersion 读全局 schema 版本（lease 校验用）。
func (s *CatalogStore) SchemaVersion(ctx context.Context) (uint64, error) {
	res, err := s.cli.Do(ctx, "GET", keycodec.SchemaVerKey())
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return strconv.ParseUint(fmt.Sprint(res), 10, 64)
}

// ListTables 返回表注册表（c:tables Hash，field=表名）。
func (s *CatalogStore) ListTables(ctx context.Context) ([]string, error) {
	res, err := s.cli.Do(ctx, "HKEYS", keycodec.TableRegistryKey())
	if err != nil {
		return nil, err
	}
	var names []string
	switch v := res.(type) {
	case []string:
		names = v
	case []any:
		for _, e := range v {
			names = append(names, fmt.Sprint(e))
		}
	}
	return names, nil
}

// RegisterTable / UnregisterTable 维护表注册表（DDL 路径调用）。
func (s *CatalogStore) RegisterTable(ctx context.Context, table string) error {
	_, err := s.cli.Do(ctx, "HSET", keycodec.TableRegistryKey(), table, 1)
	return err
}

// UnregisterTable 从注册表移除。
func (s *CatalogStore) UnregisterTable(ctx context.Context, table string) error {
	_, err := s.cli.Do(ctx, "HDEL", keycodec.TableRegistryKey(), table)
	return err
}

// asStringMap 把 HGETALL 的两种返回形态（map 或扁平数组）归一为 map。
func asStringMap(res any) (map[string]string, error) {
	switch v := res.(type) {
	case map[string]string:
		return v, nil
	case map[any]any:
		m := make(map[string]string, len(v))
		for k, val := range v {
			m[fmt.Sprint(k)] = fmt.Sprint(val)
		}
		return m, nil
	case []any:
		if len(v)%2 != 0 {
			return nil, fmt.Errorf("hgetall: odd element count %d", len(v))
		}
		m := make(map[string]string, len(v)/2)
		for i := 0; i < len(v); i += 2 {
			m[fmt.Sprint(v[i])] = fmt.Sprint(v[i+1])
		}
		return m, nil
	case nil:
		return map[string]string{}, nil
	}
	return nil, fmt.Errorf("hgetall: unexpected reply type %T", res)
}
