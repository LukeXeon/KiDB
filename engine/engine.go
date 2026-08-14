// Package engine 把 go-mysql-server 引擎绑定到 KiDB 内核：
// DatabaseProvider/Database/Table/Index/编辑器 的实现全部落在 Catalog + exec + txguard 上。
// 对应文档 docs/02 §2.5（DML 路径）。
package engine

import (
	"fmt"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"kidb"
	"kidb/exec"
	"kidb/meta"
	"kidb/script"
	"kidb/txguard"
)

// Deps 是引擎层对内核的依赖面。
type Deps struct {
	Client kidb.Client
	Reg    *script.Registry
	Cache  *meta.CatalogCache
	Store  *meta.CatalogStore
	Exec   *exec.Executor
	Guard  *txguard.Guard
}

// Build 组装引擎：默认分析器 + KiDB DatabaseProvider。
func Build(d Deps) (*sqle.Engine, *Provider, error) {
	pro := NewProvider(d)
	eng := sqle.NewDefault(pro)
	return eng, pro, nil
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
