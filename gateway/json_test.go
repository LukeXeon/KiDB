package gateway

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestJSONColumnWire JSON 列端到端（msgpack 存储形态，docs/03 §3.4）：
// 写入 → 读回语义等价（key 序归一），默认 TTL 路径无碍。
func TestJSONColumnWire(t *testing.T) {
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
	execSQL("CREATE TABLE docs (id BIGINT NOT NULL, body JSON, PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	execSQL(`INSERT INTO docs (id, body) VALUES (1, '{"z": 1, "a": {"x": [1, 2.5, null]}}')`)

	var body string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT body FROM docs WHERE id = 1").Scan(&body))
	require.JSONEq(t, `{"z":1,"a":{"x":[1,2.5,null]}}`, body, "JSON 读回语义等价（key 序归一）")

	// 非法 JSON 文本必须报错（gms Convert 拒绝）
	_, err = db.ExecContext(ctx, `INSERT INTO docs (id, body) VALUES (2, '{bad')`)
	require.Error(t, err, "非法 JSON 必须报错")

	// NULL JSON
	execSQL("INSERT INTO docs (id, body) VALUES (3, NULL)")
	var nb sql.NullString
	require.NoError(t, db.QueryRowContext(ctx, "SELECT body FROM docs WHERE id = 3").Scan(&nb))
	require.False(t, nb.Valid)
}
