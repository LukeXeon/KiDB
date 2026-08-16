package gateway

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestTTLColumnWire _ttl 伪列端到端（docs/07 §7.1）：
// 写入 >0 设行 TTL / 0 覆盖表级默认 / NULL 承默认 / <0 软删除；
// 读 = 剩余 TTL 秒自省；SELECT * 含伪列（gms 无隐藏列机制的诚实取舍）；
// UPDATE 保留剩余 TTL。
func TestTTLColumnWire(t *testing.T) {
	dsn, _, m, cleanup := newTestServer(t)
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
	execSQL("CREATE TABLE ttl_t (uid BIGINT NOT NULL, city VARCHAR(32), PRIMARY KEY (uid)) COMMENT 'kidb:{\"default_ttl\":3600}'")

	// 显式 TTL=60s
	execSQL("INSERT INTO ttl_t (uid, city, _ttl) VALUES (1, 'a', 60)")
	// 显式 0 = 无 TTL（覆盖表级默认 3600）
	execSQL("INSERT INTO ttl_t (uid, city, _ttl) VALUES (2, 'b', 0)")
	// 不提及 _ttl = 承表级默认
	execSQL("INSERT INTO ttl_t (uid, city) VALUES (3, 'c')")

	ttlOf := func(uid int) int64 {
		t.Helper()
		var ttl sql.NullInt64
		require.NoError(t, db.QueryRowContext(ctx, "SELECT _ttl FROM ttl_t WHERE uid = ?", uid).Scan(&ttl))
		require.True(t, ttl.Valid, "_ttl 应有值（行存活）")
		return ttl.Int64
	}

	require.InDelta(t, 60, ttlOf(1), 5, "显式 _ttl=60 读回剩余 ~60")
	require.Equal(t, int64(-1), ttlOf(2), "_ttl=0 → 无 TTL（-1）")
	require.InDelta(t, 3600, ttlOf(3), 5, "承表级默认 3600")

	// UPDATE 不提及 _ttl → 保留剩余 TTL（不重置为表级默认）
	execSQL("UPDATE ttl_t SET city = 'a2' WHERE uid = 1")
	require.InDelta(t, 60, ttlOf(1), 5, "UPDATE 保留剩余 TTL")

	// 显式重设
	execSQL("UPDATE ttl_t SET _ttl = 120 WHERE uid = 1")
	require.InDelta(t, 120, ttlOf(1), 5, "显式重设 _ttl")

	// SELECT * 含 _ttl 列，值为真实剩余 TTL（gms 会把 * 展开为显式投影——
	// 无隐藏列机制的诚实取舍，docs/07 §7.1；代价：同行 PTTL 搭 pipeline，
	// 行级近缓存对含 _ttl 的投影自动绕过）
	rows := db.QueryRowContext(ctx, "SELECT * FROM ttl_t WHERE uid = 1")
	var uid int64
	var city string
	var ttl sql.NullInt64
	require.NoError(t, rows.Scan(&uid, &city, &ttl), "SELECT * 应为 3 列（含 _ttl）")
	require.True(t, ttl.Valid && ttl.Int64 > 0, "SELECT * 的 _ttl 为真实剩余 TTL")

	// 软删除：_ttl < 0 → 立即过期（1ms TTL；miniredis 过期由 FastForward 驱动——
	// 生产 Redis 物理过期即时，docs/07 §7.1）
	execSQL("INSERT INTO ttl_t (uid, city, _ttl) VALUES (9, 'z', -1)")
	m.FastForward(time.Second)
	var n int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ttl_t WHERE uid = 9").Scan(&n))
	require.Equal(t, 0, n, "软删除行应不可见")
}
