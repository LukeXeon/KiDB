package gateway

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestAggregatesNative 聚合全量走引擎（v6.0 单引擎纪律，docs/02 §2.2）：
// COUNT(*) 无过滤由 gms replaceCountStar → StatisticsTable 精确承接（exp Σ ZCOUNT）；
// 带过滤 COUNT / MIN / MAX 由引擎算子承载（结果精确）；
// 端点加速写法 ORDER BY col LIMIT 1 与 MIN/MAX 结果一致。
func TestAggregatesNative(t *testing.T) {
	dsn, _, _, cleanup := newTestServer(t)
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
	execSQL("CREATE TABLE agg (id BIGINT NOT NULL, v INT, PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	execSQL("INSERT INTO agg (id, v) VALUES (1, 10), (2, 20), (3, 30)")

	// COUNT(*) 无过滤：replaceCountStar → StatisticsTable（不经 PartitionRows）
	var n int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM agg").Scan(&n))
	require.Equal(t, 3, n)

	// 带过滤 COUNT：引擎聚合
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM agg WHERE v >= 20").Scan(&n))
	require.Equal(t, 2, n)

	// MIN/MAX：引擎聚合（精确）
	var mn, mx int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT MIN(v), MAX(v) FROM agg").Scan(&mn, &mx))
	require.Equal(t, 10, mn)
	require.Equal(t, 30, mx)

	// 端点加速写法（ORDER BY col LIMIT 1）与 MIN/MAX 一致
	var top int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT v FROM agg WHERE v >= 0 ORDER BY v DESC LIMIT 1").Scan(&top))
	require.Equal(t, 30, top)
}
