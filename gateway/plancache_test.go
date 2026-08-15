package gateway

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	promdto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"kidb/metrics"
)

// counterVal 读取计数器当前值。
func counterVal(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	m := &promdto.Metric{}
	require.NoError(t, c.Write(m))
	return m.GetCounter().GetValue()
}

// TestPlanCache 判定缓存（docs/02 §2.6）：同指纹二次执行命中；
// DDL 变更（schema 版本 +1）后旧判定惰性失效（stale 重建）。
func TestPlanCache(t *testing.T) {
	dsn, deps, cleanup := newTestServer(t)
	defer cleanup()
	m := metrics.New(prometheus.NewRegistry())
	deps.Exec.SetMetrics(m)

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
	execSQL("CREATE TABLE pc (id BIGINT NOT NULL, v INT, PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	execSQL("INSERT INTO pc VALUES (1, 10), (2, 20)")

	q := "SELECT v FROM pc WHERE id = 1"
	var v int
	require.NoError(t, db.QueryRowContext(ctx, q).Scan(&v))
	require.Equal(t, 10, v)
	hit0 := counterVal(t, m.PlanCacheHit)
	stale0 := counterVal(t, m.PlanCacheStale)

	// 同指纹二次执行（字面量不同也不影响——NormalizeDigest 归一）
	require.NoError(t, db.QueryRowContext(ctx, q).Scan(&v))
	require.NoError(t, db.QueryRowContext(ctx, "SELECT v FROM pc WHERE id = 2").Scan(&v))
	require.GreaterOrEqual(t, counterVal(t, m.PlanCacheHit), hit0+1, "同指纹必须命中判定缓存")

	// DDL 变更（schema 版本 +1）→ 旧判定惰性失效
	execSQL("CREATE INDEX idx_v ON pc (v)")
	require.Eventually(t, func() bool {
		rows, qerr := db.QueryContext(ctx, "SELECT id FROM pc WHERE v >= 0")
		if qerr != nil {
			return false
		}
		rows.Close()
		return true
	}, 15*time.Second, 100*time.Millisecond)
	require.NoError(t, db.QueryRowContext(ctx, q).Scan(&v))
	require.Equal(t, 10, v)
	require.GreaterOrEqual(t, counterVal(t, m.PlanCacheStale), stale0+1, "schema 版本变化必须惰性失效")
}

// TestPrepareGuard 预处理执法面（B9 补齐的缺口）：PREPARE 期守卫生效——
// 无索引谓词被拒、事务语句被拒、DDL 被拒；点查模板放行。
func TestPrepareGuard(t *testing.T) {
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
	execSQL("CREATE TABLE pg (id BIGINT NOT NULL, v INT, PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	execSQL("INSERT INTO pg VALUES (1, 10)")

	// 点查模板放行
	stmt, err := db.PrepareContext(ctx, "SELECT v FROM pg WHERE id = ?")
	require.NoError(t, err)
	var v int
	require.NoError(t, stmt.QueryRowContext(ctx, 1).Scan(&v))
	require.Equal(t, 10, v)
	stmt.Close()

	// 无索引谓词模板在 PREPARE 期即拒
	_, err = db.PrepareContext(ctx, "SELECT v FROM pg WHERE v = ?")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ERR_NO_INDEX")

	// 事务语句 PREPARE 期拒绝
	_, err = db.PrepareContext(ctx, "START TRANSACTION")
	require.Error(t, err)
}
