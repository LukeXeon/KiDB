package gateway

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"kidb/internal/tuning"
)

// TestGuardJoinTiers JOIN 分档执法（docs/04 §4.4）：
// 档 1（右表主键等值）放行；档 4（非主键任意关联）报错。
func TestGuardJoinTiers(t *testing.T) {
	dsn, _, cleanup := newTestServer(t)
	defer cleanup()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	execSQL := func(q string) {
		t.Helper()
		_, err := db.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}
	// 档 2 阈值压到 1：3 行的 users 超阈（默认 10 万内会被当维表放行——
	// 本测试压的是档位判定本身，不是维表通路）
	tuning.OverrideForTest(t, func(tn *tuning.Tuning) { tn.Gateway.DimensionMaxRows = 1 })
	execSQL("CREATE TABLE orders (oid BIGINT PRIMARY KEY, uid BIGINT, amount INT)")
	execSQL("CREATE TABLE users (uid BIGINT PRIMARY KEY, name VARCHAR(32))")
	execSQL("INSERT INTO orders VALUES (1, 100, 50), (2, 100, 60), (3, 200, 70)")
	execSQL("INSERT INTO users VALUES (100, 'alice'), (200, 'bob')")

	// 档 1：右表（users）主键等值 → 放行
	rows, err := db.QueryContext(ctx, "SELECT o.oid FROM orders o JOIN users u ON o.uid = u.uid")
	require.NoError(t, err, "档 1 主键查找 JOIN 必须放行")
	cnt := 0
	for rows.Next() {
		cnt++
	}
	rows.Close()
	require.Equal(t, 3, cnt)

	// 档 4：右表非主键列关联 → 报错
	_, err = db.QueryContext(ctx, "SELECT * FROM orders o JOIN users u ON o.uid = u.name")
	require.Error(t, err)
	require.Contains(t, err.Error(), "1235", "档 4 必须报 1235（ERR_UNSUPPORTED_JOIN）")

	// CROSS JOIN → 报错
	_, err = db.QueryContext(ctx, "SELECT * FROM orders CROSS JOIN users")
	require.Error(t, err)
}

// TestGuardNoIndex 无索引谓词执法（docs/04 §4.1 + docs/07 §7.4）：
// 无索引谓词默认报错；hint 与白名单放行。
func TestGuardNoIndex(t *testing.T) {
	dsn, _, cleanup := newTestServer(t)
	defer cleanup()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	execSQL := func(q string) {
		t.Helper()
		_, err := db.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}
	execSQL("CREATE TABLE ev (id BIGINT PRIMARY KEY, tag VARCHAR(32), n INT)")
	execSQL("INSERT INTO ev VALUES (1, 'a', 10), (2, 'b', 20)")

	// 无索引谓词 → ERR_NO_INDEX
	_, err = db.QueryContext(ctx, "SELECT * FROM ev WHERE tag = 'a'")
	require.Error(t, err)
	require.Contains(t, err.Error(), "无可用索引")

	// 无 WHERE 全扫 → 同纪律
	_, err = db.QueryContext(ctx, "SELECT tag FROM ev")
	require.Error(t, err)

	// FULLSCAN hint 放行
	rows, err := db.QueryContext(ctx, "SELECT /*+ FULLSCAN */ tag FROM ev")
	require.NoError(t, err, "hint 放行")
	for rows.Next() {
	}
	rows.Close()

	// UPDATE/DELETE 无 WHERE 同纪律（在白名单设置之前断言）
	_, err = db.ExecContext(ctx, "UPDATE ev SET n = 0")
	require.Error(t, err)
	_, err = db.ExecContext(ctx, "DELETE FROM ev")
	require.Error(t, err)

	// 白名单放行
	execSQL("SET GLOBAL query_allow_fullscan_tables = 'ev'")
	// 等配置传播（本实例即刻；生产多实例秒级）
	time.Sleep(100 * time.Millisecond)
	_, err = db.QueryContext(ctx, "SELECT tag FROM ev")
	require.NoError(t, err, "白名单放行")

	// 主键谓词始终放行
	require.NoError(t, func() error {
		r := db.QueryRowContext(ctx, "SELECT n FROM ev WHERE id = 1")
		var v int
		return r.Scan(&v)
	}())
}

// TestMultiRowPartialDetail 多行 DML 部分成功明细（docs/05 §5.5）+ 行体积防线。
func TestMultiRowPartialDetail(t *testing.T) {
	dsn, _, cleanup := newTestServer(t)
	defer cleanup()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	ctx := context.Background()

	execSQL := func(q string) {
		t.Helper()
		_, err := db.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}
	tuning.OverrideForTest(t, func(tn *tuning.Tuning) { tn.Txguard.MaxRowBytes = 64 }) // 行体积防线（tuning.toml）
	execSQL("CREATE TABLE mr (id BIGINT PRIMARY KEY, v VARCHAR(16)) COMMENT 'kidb:{}'")
	execSQL("INSERT INTO mr VALUES (1, 'a')")

	// 多行 INSERT 中途唯一冲突：MySQL 惯例 1062 纯错误
	// （dup 错误必须保持 UniqueKeyError 形态——ODKU 依赖，不带附加明细）
	_, err = db.ExecContext(ctx, "INSERT INTO mr VALUES (2, 'b'), (1, 'dup'), (3, 'c')")
	require.Error(t, err)
	require.Contains(t, err.Error(), "1062", "唯一冲突映射 1062")

	// 非 dup 类失败（行体积超限）携带部分成功明细（docs/05 §5.5）
	_, err = db.ExecContext(ctx, "INSERT INTO mr VALUES (4, 'd'), (5, '"+strings.Repeat("x", 100)+"')")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ERR_ROW_TOO_LARGE")
	require.Contains(t, err.Error(), "已提交", "非 dup 类失败须含部分成功明细")
}
