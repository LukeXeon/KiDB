package meta

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"kidb"
	"kidb/keycodec"
)

// CatalogStore 是 Catalog 的读写存储（docs/06 §6.1：`c:table:{table}` Hash，
// 字段 def=编码后的 TableDef、_ver=表级版本、_job=进行中的 DDL 作业）。
//
// TODO(impl): def 编码当前为 JSON；切换 msgp 代码生成版见 docs/03 §3.4
// （格式版本号内嵌，迁移走 docs/06 §6.4 演进纪律）。
// TODO(impl): Save 的 CAS 当前为读-改-写，实现期改 config_set 风格 Lua 原子 CAS。
type CatalogStore struct {
	cli kidb.Client
}

// NewCatalogStore 构造存储。
func NewCatalogStore(cli kidb.Client) *CatalogStore { return &CatalogStore{cli: cli} }

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
	if err := json.Unmarshal([]byte(raw), &def); err != nil {
		return nil, fmt.Errorf("catalog %s: decode def: %w", table, err)
	}
	ver, _ := strconv.ParseUint(fields["_ver"], 10, 64)
	def.Ver = ver
	return &def, nil
}

// Save 写表定义（带期望版本；version 由存储侧 _ver+1）。
func (s *CatalogStore) Save(ctx context.Context, def *TableDef, expectVer uint64) error {
	cur, err := s.Load(ctx, def.Name)
	if err != nil {
		return err
	}
	var curVer uint64
	if cur != nil {
		curVer = cur.Ver
	}
	if curVer != expectVer {
		return fmt.Errorf("%w: catalog %s expect _ver=%d got %d", kidb.ErrStaleMetadata, def.Name, expectVer, curVer)
	}
	raw, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("catalog %s: encode def: %w", def.Name, err)
	}
	key := keycodec.CatalogKey(def.Name)
	if _, err := s.cli.Do(ctx, "HSET", key, "def", string(raw)); err != nil {
		return err
	}
	if _, err := s.cli.Do(ctx, "HINCRBY", key, "_ver", 1); err != nil {
		return err
	}
	// 全局 schema 版本递增（docs/06 §6.2：plan cache 与 lease 的失效锚点）。
	_, err = s.cli.Do(ctx, "INCR", keycodec.SchemaVerKey())
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
