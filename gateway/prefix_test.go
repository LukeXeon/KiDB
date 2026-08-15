package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestPrefixSearchWire: prefix search end to end (docs/04 §4.5, docs/02 §2.4 prefix_copy).
// `col LIKE 'abc%'` -> lex copy ZRANGEBYLEX merge path; results globally lex-ordered
// and consistent with full scan; non-const-prefix LIKE keeps the no-index discipline.
func TestPrefixSearchWire(t *testing.T) {
	dsn, _, cleanup := newTestServer(t)
	defer cleanup()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	execSQL := func(q string) {
		t.Helper()
		_, err := db.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}
	execSQL("CREATE TABLE shops (id BIGINT NOT NULL, name VARCHAR(64), city VARCHAR(32), PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	execSQL("CREATE INDEX idx_city ON shops (city)")
	require.Eventually(t, func() bool {
		rows, qerr := db.QueryContext(ctx, "SELECT id FROM shops WHERE city = '__probe__'")
		if qerr != nil {
			return false
		}
		rows.Close()
		return true
	}, 15*time.Second, 100*time.Millisecond, "wait index backfill")

	cities := []string{"shanghai", "shangrao", "shenzhen", "beijing", "shaoxing", "shenyang"}
	for i := 1; i <= 120; i++ {
		execSQL(fmt.Sprintf("INSERT INTO shops (id, name, city) VALUES (%d, 'shop%d', '%s')", i, i, cities[i%len(cities)]))
	}

	// const prefix: 'sh%' hits shanghai/shangrao/shaoxing/shenzhen/shenyang (5x20=100 rows)
	rows, err := db.QueryContext(ctx, "SELECT name, city FROM shops WHERE city LIKE 'sh%'")
	require.NoError(t, err)
	var got []string
	for rows.Next() {
		var name, city string
		require.NoError(t, rows.Scan(&name, &city))
		_ = name
		got = append(got, city)
	}
	rows.Close()
	require.Len(t, got, 100)
	for _, c := range got {
		require.True(t, c == "shanghai" || c == "shangrao" || c == "shaoxing" || c == "shenzhen" || c == "shenyang", "city %q must not match", c)
	}

	// ORDER BY the prefix column: gms drops Sort -> merge output must be globally lex-ordered
	rows, err = db.QueryContext(ctx, "SELECT city FROM shops WHERE city LIKE 'sh%' ORDER BY city ASC")
	require.NoError(t, err)
	var ord []string
	for rows.Next() {
		var c string
		require.NoError(t, rows.Scan(&c))
		ord = append(ord, c)
	}
	rows.Close()
	require.Len(t, ord, 100)
	for i := 1; i < len(ord); i++ {
		require.LessOrEqual(t, ord[i-1], ord[i], "lex order broken at row %d", i)
	}

	// infix LIKE: no usable index -> no-index discipline error
	_, err = db.QueryContext(ctx, "SELECT name FROM shops WHERE city LIKE '%hai'")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ERR_NO_INDEX")

	// prefix LIKE on a column without prefix copy: also rejected
	_, err = db.QueryContext(ctx, "SELECT city FROM shops WHERE name LIKE 'shop1%'")
	require.Error(t, err)
}
