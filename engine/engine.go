// Package engine 把 go-mysql-server 引擎绑定到 KiDB 内核：
// DatabaseProvider/Database/Table/Index/编辑器/Session 的实现全部落在
// Catalog + exec + txguard 上（docs/02）。
//
// v6.0 纪律（用户裁决）：**gms 分析器零规则变更**——公开扩展点之外的内部
// 规则一个不动；MySQL 习惯校验（如 TEXT/BLOB 索引前缀长度）靠"Catalog 忠实
// 记录 gms 类型"天然兼容（typetext.go），而非卸载规则。
package engine

import (
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"

	"kidb"
	"kidb/exec"
	"kidb/meta"
	"kidb/script"
	"kidb/txguard"
)

// Deps 是引擎层对内核的依赖面。
type Deps struct {
	Client kidb.KvClient
	Reg    *script.Registry
	Cache  *meta.CatalogCache
	Store  *meta.CatalogStore
	Exec   *exec.Executor
	Guard  *txguard.Guard

	// FullscanGate 全表遍历闸门（docs/07 §7.4 访问控制 + docs/04 §4.1 无索引
	// 纪律）：引擎层全扫（PartitionRows 无索引可用）前 consult；返回 nil = 放行。
	// nil = 一律拒绝（测试默认严格；生产装配：小表自动放行 + 白名单放行并告警）。
	FullscanGate func(ctx *sql.Context, table string, rows uint64) error
}

// Build 组装引擎：默认分析器（零规则变更）+ KiDB DatabaseProvider +
// 语义开关注册（docs/02 §2.7）。
func Build(d Deps) (*sqle.Engine, *Provider, error) {
	RegisterSysvars()
	pro := NewProvider(d)
	ab := analyzer.NewBuilder(pro)
	eng := sqle.New(ab.Build(), nil)
	return eng, pro, nil
}
