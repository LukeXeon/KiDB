package engine

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	vtmysql "github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"

	"kidb/config"
	"kidb/internal/redistest"
)

// TestSysvarsPersistOnSetGlobal SET GLOBAL → NotifyChanged → cfg:global 持久化
// （docs/02 §2.7 数据流）；ro 账号在钩子内被拒。
func TestSysvarsPersistOnSetGlobal(t *testing.T) {
	RegisterSysvars()
	cli, reg, _ := redistest.New(t)
	store := config.New(cli, reg, "test")

	rw := &Session{BaseSession: sql.NewBaseSession(), Role: "rw", Cfg: store}
	ctx := sql.NewContext(context.Background(), sql.WithSession(rw))

	require.NoError(t, sql.SystemVariables.SetGlobal(ctx, VarFullscanAllowlist, "t1,t2"))
	require.NoError(t, sql.SystemVariables.SetGlobal(ctx, VarReplicaRead, true))

	// cfg:global 已持久化（config 校验器口径：布尔存 "true"/"false"）
	v, set, err := store.Get(context.Background(), VarFullscanAllowlist)
	require.NoError(t, err)
	require.True(t, set)
	require.Equal(t, "t1,t2", v)
	v, set, err = store.Get(context.Background(), VarReplicaRead)
	require.NoError(t, err)
	require.True(t, set)
	require.Equal(t, "true", v)

	// 读侧单一事实源 = gms 注册表
	require.True(t, SysvarBool(VarReplicaRead))
	require.Equal(t, "t1,t2", SysvarString(VarFullscanAllowlist))

	// ro 账号 SET GLOBAL 被拒（1290）
	ro := &Session{BaseSession: sql.NewBaseSession(), Role: "ro", Cfg: store}
	roCtx := sql.NewContext(context.Background(), sql.WithSession(ro))
	err = sql.SystemVariables.SetGlobal(roCtx, VarFullscanAllowlist, "t3")
	require.Error(t, err)

	// 测试尾声：复位，避免串扰同进程其他测试
	require.NoError(t, sql.SystemVariables.SetGlobal(ctx, VarFullscanAllowlist, ""))
	require.NoError(t, sql.SystemVariables.SetGlobal(ctx, VarReplicaRead, false))
}

// TestFullscanGate 闸门三态：小表自动放行 / 白名单放行 / 大表拒绝。
func TestFullscanGate(t *testing.T) {
	RegisterSysvars()
	gate := NewFullscanGate(nil)
	ctx := sql.NewContext(context.Background())

	// 小表（< dimension_max_rows）自动放行
	require.NoError(t, gate(ctx, "small_t", 100))

	// 大表未白名单 → ERR_NO_INDEX
	require.Error(t, gate(ctx, "big_t", 1_000_000))

	// 白名单放行（大小写不敏感）
	require.NoError(t, sql.SystemVariables.SetGlobal(ctx, VarFullscanAllowlist, "BIG_T, other"))
	require.NoError(t, gate(ctx, "big_t", 1_000_000))
	require.Error(t, gate(ctx, "not_listed", 1_000_000))

	require.NoError(t, sql.SystemVariables.SetGlobal(ctx, VarFullscanAllowlist, ""))
}

// TestSessionTransactionSemantics 事务语义（docs/02 §2.1）：
// 隐式 autocommit（GetTransaction()==nil）→ 占位 tx 放行；
// 显式 BEGIN（ctx 已有活动 tx，gms plan.StartTransaction 调用序）→ 1235。
func TestSessionTransactionSemantics(t *testing.T) {
	s := &Session{BaseSession: sql.NewBaseSession()}
	ctx := sql.NewContext(context.Background(), sql.WithSession(s))

	// 隐式 autocommit：放行（gms 每语句自动开启）
	tx, err := s.StartTransaction(ctx, sql.ReadWrite)
	require.NoError(t, err)
	require.NotNil(t, tx)
	require.NoError(t, s.CommitTransaction(ctx, tx))

	// 显式 BEGIN：gms plan.StartTransaction 先 Commit 再 Start（不清空 ctx 事务）
	ctx.SetTransaction(tx)
	_, err = s.StartTransaction(ctx, sql.ReadWrite)
	requireSQLErrorCode(t, err, 1235)

	// SAVEPOINT 恒报错
	requireSQLErrorCode(t, s.CreateSavepoint(ctx, tx, "sp1"), 1235)
}

// requireSQLErrorCode 断言引擎边界错误携带指定 MySQL 错误码
// （sqlErr 翻译为 *mysql.SQLError 后 errors.Is 链断 sentinel——按码断言）。
func requireSQLErrorCode(t *testing.T, err error, code int) {
	t.Helper()
	require.Error(t, err)
	var se *vtmysql.SQLError
	require.ErrorAs(t, err, &se)
	require.Equal(t, code, se.Num)
}
