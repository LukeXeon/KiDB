// Package engine 把 go-mysql-server 引擎绑定到 KiDB 内核：
// DatabaseProvider/Database/Table/Index/编辑器 的实现全部落在 Catalog + exec + txguard 上。
// 对应文档 docs/02 §2.5（DML 路径）。
package engine

import (
	"fmt"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/types"

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
}

// Build 组装引擎：自定义分析器 + KiDB DatabaseProvider。
//
// 移除的规则（与 KiDB 索引模型冲突的 MySQL 习惯校验）：resolveAlterColumn
// （OnceBeforeDefault）——它按"TEXT/BLOB 列索引必须带前缀长度"校验 ALTER/建索引，
// 而 KiDB 字符串列经桶模型索引、无前缀长度概念（我们的 Schema 把字符串映射为
// LongText，触发该校验）。KiDB 侧的索引校验（形态/覆盖/互斥）在 ddl 包完成。
func Build(d Deps) (*sqle.Engine, *Provider, error) {
	pro := NewProvider(d)
	ab := analyzer.NewBuilder(pro)
	ab.RemoveOnceBeforeRule(mustRuleID("resolveAlterColumn"))
	eng := sqle.New(ab.Build(), nil)
	return eng, pro, nil
}

// mustRuleID 按名解析 gms 分析器规则 id（规则 id 常量未导出；
// RuleId 的 String() 由 stringer 生成，锁版本 v0.20 钉死）。
func mustRuleID(name string) analyzer.RuleId {
	for i := analyzer.RuleId(0); i < 512; i++ {
		if strings.EqualFold(i.String(), name) {
			return i
		}
	}
	panic("engine: gms 规则不存在（升级 gms 需复核）: " + name)
}

// goType 把 KiDB 列类型映射为 gms 类型（docs/02 §2.4 白名单）。
func goType(ct meta.ColumnType) (sql.Type, error) {
	switch ct {
	case meta.ColInt:
		return types.Int64, nil
	case meta.ColFloat:
		return types.Float64, nil
	case meta.ColString:
		return types.LongText, nil
	case meta.ColBytes:
		return types.LongBlob, nil
	case meta.ColTimestamp:
		return types.Timestamp, nil
	case meta.ColJSON:
		return types.JSON, nil
	}
	return nil, fmt.Errorf("engine: unknown column type %v", ct)
}
