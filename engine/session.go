package engine

import (
	"github.com/dolthub/go-mysql-server/sql"

	"kidb"
	"kidb/config"
)

// Session 是 KiDB 的 gms 会话（v6.0：执法面全部内建于框架扩展点，
// 网关无 handler 包装层，docs/02 §2.4）。
type Session struct {
	*sql.BaseSession
	Role string        // "rw" / "ro"（连接期由账号表注入，docs/02 §2.8）
	Cfg  *config.Store // 本实例的配置存储（SET GLOBAL 持久化桥，docs/10 §10.2）
}

// SessionRole 取会话角色（ro 执法读点）。
func SessionRole(ctx *sql.Context) string {
	if s, ok := ctx.Session.(*Session); ok {
		return s.Role
	}
	return "rw" // 内部构造的会话（测试/带外工具）默认读写
}

// SessionCfg 取会话携带的配置存储（sysvar NotifyChanged 的持久化桥）。
func SessionCfg(ctx *sql.Context) *config.Store {
	if s, ok := ctx.Session.(*Session); ok {
		return s.Cfg
	}
	return nil
}

// RejectRO ro 账号写操作执法（docs/02 §2.8）：写路径各入口（编辑器/DDL 接口/
// SET GLOBAL 钩子）统一调用。返回 nil = 放行。
func RejectRO(ctx *sql.Context) error {
	if SessionRole(ctx) == "ro" {
		return kidb.ErrReadOnly
	}
	return nil
}

// ==== TransactionSession（docs/02 §2.1：缓存定位无事务）====
//
// gms 对不实现 TransactionSession 的会话**静默 no-op** BEGIN（客户端误以为
// 事务生效——隐性部分提交陷阱），故必须实现。但 gms 的 autocommit 生命周期
// 对每条语句自动 StartTransaction/CommitTransaction（beginTransaction +
// TransactionCommittingIter）——全部报错会杀死所有语句。
//
// 判别式（gms v0.20 调用序实证）：
//   - 隐式路径 engine.beginTransaction 以 GetTransaction()==nil 为守卫；
//   - 显式 BEGIN/START TRANSACTION（plan.StartTransaction）先 Commit 当前 tx
//     再 StartTransaction，期间**不清空** ctx 事务——
//     故 StartTransaction 入口处 GetTransaction()!=nil ⟺ 显式事务语句。
//
// 显式事务语句 → 1235 显式报错；隐式 autocommit → 占位 tx（无真实事务语义，
// 写入原子性由单 slot Lua 独立保证，docs/01 §1.4）。显式 COMMIT/ROLLBACK
// 在 BEGIN 已报错的前提下到达时 ctx 无活动 tx（plan 节点 no-op），与 MySQL
// autocommit 会话的 COMMIT/ROLLBACK 空成功行为一致。

// noopTx 是 gms autocommit 生命周期的占位事务（不承载任何状态或语义）。
type noopTx struct{}

func (noopTx) String() string { return "kidb-autocommit" }

// IsReadOnly 否（gms Transaction 接口要求；只读优化对本方案无意义——
// 读路径一致性由回表校验保证）。
func (noopTx) IsReadOnly() bool { return false }

// StartTransaction 隐式开启返回占位 tx；显式 BEGIN 报错（1235）。
func (s *Session) StartTransaction(ctx *sql.Context, t sql.TransactionCharacteristic) (sql.Transaction, error) {
	if ctx.GetTransaction() != nil {
		return nil, sqlErr(kidb.ErrUnsupported) // 显式 BEGIN/START TRANSACTION
	}
	return noopTx{}, nil
}

// CommitTransaction 占位提交（每语句收尾由 gms 调用；占位 tx 直接放行）。
func (s *Session) CommitTransaction(ctx *sql.Context, tx sql.Transaction) error {
	return nil
}

// Rollback 占位回滚（语句级原子性已在单 slot Lua 内保证，无暂存变更可回滚）。
func (s *Session) Rollback(ctx *sql.Context, tx sql.Transaction) error {
	return nil
}

// CreateSavepoint 报错（无显式事务则无合法到达路径）。
func (s *Session) CreateSavepoint(ctx *sql.Context, tx sql.Transaction, name string) error {
	return sqlErr(kidb.ErrUnsupported)
}

// RollbackToSavepoint 报错。
func (s *Session) RollbackToSavepoint(ctx *sql.Context, tx sql.Transaction, name string) error {
	return sqlErr(kidb.ErrUnsupported)
}

// ReleaseSavepoint 报错。
func (s *Session) ReleaseSavepoint(ctx *sql.Context, tx sql.Transaction, name string) error {
	return sqlErr(kidb.ErrUnsupported)
}

var _ sql.TransactionSession = (*Session)(nil)
