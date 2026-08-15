package gateway

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestExplainWire EXPLAIN 的 KiDB 计划展示（docs/02 §2.8）。
func TestExplainWire(t *testing.T) {
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
	execSQL("CREATE TABLE ex (id BIGINT NOT NULL, tag VARCHAR(32), age INT, PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	// 单 _job 槽位：索引串行建，各等回填完成
	waitIdx := func(probe string) {
		t.Helper()
		require.Eventually(t, func() bool {
			rows, qerr := db.QueryContext(ctx, probe)
			if qerr != nil {
				return false
			}
			rows.Close()
			return true
		}, 15*time.Second, 100*time.Millisecond)
	}
	execSQL("CREATE INDEX idx_tag ON ex (tag)")
	waitIdx("SELECT id FROM ex WHERE tag = '__probe__'")
	execSQL("CREATE INDEX idx_age ON ex (age)")
	waitIdx("SELECT id FROM ex WHERE age >= 0")

	planOf := func(q string) map[string]string {
		t.Helper()
		rows, err := db.QueryContext(ctx, q)
		require.NoError(t, err, q)
		defer rows.Close()
		out := map[string]string{}
		for rows.Next() {
			var item, detail string
			require.NoError(t, rows.Scan(&item, &detail))
			out[item] = detail
		}
		return out
	}

	// 主键点查
	m := planOf("EXPLAIN SELECT * FROM ex WHERE id = 7")
	require.Equal(t, "point_get", m["plan"])

	// 等值索引
	m = planOf("EXPLAIN SELECT tag FROM ex WHERE tag = 'x'")
	require.Equal(t, "eq_lookup", m["plan"])
	require.Equal(t, "idx_tag", m["index"])

	// 范围索引（有序归并）
	m = planOf("EXPLAIN SELECT id FROM ex WHERE age > 30 ORDER BY age LIMIT 5")
	require.Equal(t, "range_lookup(ordered)", m["plan"])
	require.Equal(t, "idx_age", m["index"])

	// COUNT(*) 快速路径（EXPLAIN 不得真执行——无 fanout 行数即可证）
	m = planOf("EXPLAIN SELECT COUNT(*) FROM ex")
	require.Equal(t, "fastpath:count_star", m["plan"])

	// 无索引谓词 → fullscan + 守卫判定
	m = planOf("EXPLAIN SELECT id FROM ex WHERE tag > 'a'")
	require.Equal(t, "fullscan", m["plan"])
	require.Contains(t, m["guard"], "ERR_NO_INDEX")

	// EXPLAIN UPDATE 报 1235
	_, err = db.QueryContext(ctx, "EXPLAIN UPDATE ex SET tag = 'y' WHERE id = 1")
	require.Error(t, err)
}
