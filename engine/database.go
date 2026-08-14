package engine

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"kidb"
)

// errUnsupported 是本包的定位外错误出口（映射 1235）。
var errUnsupported = kidb.ErrUnsupported

// Provider 实现 sql.DatabaseProvider。v1 为扁平命名空间：
// USE 任意库名被接受并记录，表全局唯一（命名空间前缀隔离列入后续，docs/02 §2.5）。
type Provider struct {
	deps Deps
}

// NewProvider 构造。
func NewProvider(d Deps) *Provider { return &Provider{deps: d} }

// Database 返回逻辑库（扁平命名空间：任何名字都解析到同一库）。
func (p *Provider) Database(ctx *sql.Context, name string) (sql.Database, error) {
	return &Database{name: name, p: p}, nil
}

// HasDatabase 恒真（扁平命名空间）。
func (p *Provider) HasDatabase(ctx *sql.Context, name string) bool { return true }

// AllDatabases 返回默认库（SHOW DATABASES 用）。
func (p *Provider) AllDatabases(ctx *sql.Context) []sql.Database {
	return []sql.Database{&Database{name: "kidb", p: p}}
}

// Database 是 Catalog 驱动的逻辑库。
type Database struct {
	name string
	p    *Provider
}

// Name 库名。
func (d *Database) Name() string { return d.name }

// GetTableInsensitive 按名取表（Catalog 缓存，大小写不敏感）。
func (d *Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	def, err := d.p.deps.Cache.Get(ctx, tblName)
	if err != nil {
		return nil, false, err
	}
	if def == nil {
		return nil, false, nil
	}
	return NewTable(def, d.p.deps), true, nil
}

// GetTableNames 从表注册表生成（docs/02 §2.10：SHOW TABLES 由此来）。
func (d *Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	names, err := d.p.deps.Store.ListTables(ctx)
	if err != nil {
		return nil, err
	}
	lower := names[:0]
	for _, n := range names {
		lower = append(lower, strings.ToLower(n))
	}
	return lower, nil
}

var _ sql.Database = (*Database)(nil)

// === 能力探针空实现：gms 分析器在解析阶段会探测视图/触发器支持 ===
// （docs/01 §1.2：视图/触发器超出缓存查询层定位，DDL 面明确报错，探测面返回空）

// AllViews 无视图。
func (d *Database) AllViews(ctx *sql.Context) ([]sql.ViewDefinition, error) { return nil, nil }

// GetViewDefinition 无视图定义。
func (d *Database) GetViewDefinition(ctx *sql.Context, viewName string) (sql.ViewDefinition, bool, error) {
	return sql.ViewDefinition{}, false, nil
}

// CreateView 拒绝（定位外）。
func (d *Database) CreateView(ctx *sql.Context, name, selectStatement, createViewStmt string) error {
	return fmt.Errorf("%w: VIEW 超出缓存查询层定位", errUnsupported)
}

// DropView 拒绝。
func (d *Database) DropView(ctx *sql.Context, name string) error {
	return fmt.Errorf("%w: VIEW 超出缓存查询层定位", errUnsupported)
}

// GetTriggers 无触发器。
func (d *Database) GetTriggers(ctx *sql.Context) ([]sql.TriggerDefinition, error) { return nil, nil }

// CreateTrigger 拒绝。
func (d *Database) CreateTrigger(ctx *sql.Context, definition sql.TriggerDefinition) error {
	return fmt.Errorf("%w: TRIGGER 超出缓存查询层定位", errUnsupported)
}

// DropTrigger 拒绝。
func (d *Database) DropTrigger(ctx *sql.Context, name string) error {
	return fmt.Errorf("%w: TRIGGER 超出缓存查询层定位", errUnsupported)
}

var (
	_ sql.ViewDatabase    = (*Database)(nil)
	_ sql.TriggerDatabase = (*Database)(nil)
)
