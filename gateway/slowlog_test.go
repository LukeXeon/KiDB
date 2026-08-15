package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"kidb"
	"kidb/engine"
	"kidb/exec"
	"kidb/internal/redistest"
	"kidb/meta"
	"kidb/txguard"
)

// slogCapture 测试用日志捕获 Handler。
type slogCapture struct {
	mu  sync.Mutex
	rec []slog.Record
}

func (c *slogCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *slogCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rec = append(c.rec, r)
	return nil
}
func (c *slogCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *slogCapture) WithGroup(string) slog.Handler      { return c }

func (c *slogCapture) contains(sub string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.rec {
		if strings.Contains(r.Message, sub) {
			return true
		}
	}
	return false
}

// TestSlowQueryLog 慢查询日志（docs/10 §10.4）：
// 超阈值（进程级阈值，测试设为 ~0）查询落日志（指纹/路由/行数/耗时）；
// 全扫放行 → 与阈值无关的强制告警。
func TestSlowQueryLog(t *testing.T) {
	cap_ := &slogCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(cap_))
	defer slog.SetDefault(prev)

	cli, reg, _ := redistest.New(t)
	store := meta.NewCatalogStore(cli, reg)
	deps := engine.Deps{
		Client: cli,
		Reg:    reg,
		Store:  store,
		Cache:  meta.NewCatalogCache(store),
		Exec:   exec.New(cli, reg),
		Guard:  txguard.New(cli, reg, nil),
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv, err := newServerWithListener(deps, kidb.Bootstrap{
		Accounts: []kidb.Account{{User: "root", Host: "%", Password: "", Role: "rw"}},
	}, l)
	require.NoError(t, err)
	srv.slowQueryThreshold = time.Nanosecond // 全量记录
	go srv.Start()
	defer srv.Close()
	time.Sleep(50 * time.Millisecond)

	db, err := sql.Open("mysql", fmt.Sprintf("root:@tcp(%s)/kidb", l.Addr().String()))
	require.NoError(t, err)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	execSQL := func(q string) {
		t.Helper()
		_, err := db.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}
	execSQL("CREATE TABLE sl (id BIGINT NOT NULL, v INT, PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	execSQL("INSERT INTO sl VALUES (1, 10), (2, 20)")

	var n int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sl").Scan(&n))
	require.Equal(t, 2, n)
	require.Eventually(t, func() bool { return cap_.contains("慢查询") }, 3*time.Second, 50*time.Millisecond,
		"超阈值查询必须落慢查询日志")

	// 全扫放行（hint）→ 强制告警（阈值恢复默认后也应告警）
	srv.slowQueryThreshold = time.Hour
	rows, err := db.QueryContext(ctx, "SELECT /*+ FULLSCAN */ v FROM sl")
	require.NoError(t, err)
	for rows.Next() {
	}
	rows.Close()
	require.Eventually(t, func() bool { return cap_.contains("全扫查询") }, 3*time.Second, 50*time.Millisecond,
		"全扫放行必须强制告警（与阈值无关）")
}
