package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"kidb"
	"kidb/engine"
	"kidb/exec"
	"kidb/internal/redistest"
	"kidb/meta"
	"kidb/txguard"
)

// newTestServer 在 miniredis 上起完整网关（随机端口），返回 DSN。
func newTestServer(t *testing.T) (string, func()) {
	t.Helper()
	cli, reg, _ := redistest.New(t)

	store := meta.NewCatalogStore(cli)
	deps := engine.Deps{
		Client: cli,
		Reg:    reg,
		Store:  store,
		Cache:  meta.NewCatalogCache(store),
		Exec:   exec.New(cli),
		Guard:  txguard.New(cli, reg),
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// 借 server.Config.Listener 走随机端口（gms 支持自定义 listener）
	srv, err := newServerWithListener(deps, kidb.Bootstrap{
		Accounts: []kidb.Account{{User: "root", Host: "%", Password: "", Role: "rw"}},
	}, l)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	time.Sleep(50 * time.Millisecond) // 等监听就绪

	dsn := fmt.Sprintf("root:@tcp(%s)/kidb", l.Addr().String())
	return dsn, func() { srv.Close() }
}

// TestGatewaySmoke 端到端冒烟：真实 MySQL 驱动 → DDL → CRUD → 预处理。
// 覆盖 docs/02 §2.10 的最小客户端路径（go-sql-driver）。
func TestGatewaySmoke(t *testing.T) {
	dsn, cleanup := newTestServer(t)
	defer cleanup()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	execSQL := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("Exec %q: %v", q, err)
		}
	}

	execSQL("CREATE TABLE users (uid BIGINT NOT NULL, city VARCHAR(32), age INT, PRIMARY KEY (uid)) COMMENT 'kidb:{}'")
	execSQL("CREATE INDEX idx_city ON users (city)")
	execSQL("INSERT INTO users (uid, city, age) VALUES (1, 'shanghai', 30), (2, 'beijing', 25), (3, 'shanghai', 40)")

	// 主键点查
	var city string
	if err := db.QueryRowContext(ctx, "SELECT city FROM users WHERE uid = 1").Scan(&city); err != nil {
		t.Fatalf("点查: %v", err)
	}
	if city != "shanghai" {
		t.Fatalf("city = %q", city)
	}

	// 等值索引查询
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE city = 'shanghai'").Scan(&n); err != nil {
		t.Fatalf("COUNT: %v", err)
	}
	if n != 2 {
		t.Fatalf("COUNT = %d, want 2", n)
	}

	// 范围索引 + ORDER BY + LIMIT（引擎层算子）
	rows, err := db.QueryContext(ctx, "SELECT uid FROM users WHERE age >= 26 ORDER BY age DESC LIMIT 1")
	if err != nil {
		t.Fatalf("范围+排序: %v", err)
	}
	defer rows.Close()
	var got []int64
	for rows.Next() {
		var uid int64
		if err := rows.Scan(&uid); err != nil {
			t.Fatal(err)
		}
		got = append(got, uid)
	}
	if len(got) != 1 || got[0] != 3 {
		t.Fatalf("ORDER BY LIMIT = %v, want [3]", got)
	}

	// 更新 + 唯一约束外的普通路径
	execSQL("UPDATE users SET city = 'hangzhou' WHERE uid = 2")
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE city = 'beijing'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("更新后 COUNT = %d, want 0", n)
	}

	// 删除
	execSQL("DELETE FROM users WHERE uid = 3")
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("删除后 COUNT = %d, want 2", n)
	}

	// 预处理语句（docs/02 §2.5）
	stmt, err := db.PrepareContext(ctx, "SELECT age FROM users WHERE uid = ?")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()
	var age int
	if err := stmt.QueryRowContext(ctx, 1).Scan(&age); err != nil {
		t.Fatalf("Stmt query: %v", err)
	}
	if age != 30 {
		t.Fatalf("age = %d", age)
	}

	// 事务语句拒绝（docs/02 §2.1）
	if _, err := db.ExecContext(ctx, "BEGIN"); err == nil {
		t.Fatal("BEGIN 必须报错 1235")
	}

	// SHOW TABLES（握手兼容面）
	var tbl string
	if err := db.QueryRowContext(ctx, "SHOW TABLES").Scan(&tbl); err != nil {
		t.Fatalf("SHOW TABLES: %v", err)
	}
	if tbl != "users" {
		t.Fatalf("SHOW TABLES = %q", tbl)
	}
}

// TestGatewayRO ro 账号执法（docs/02 §2.9）。
func TestGatewayRO(t *testing.T) {
	cli, reg, _ := redistest.New(t)
	store := meta.NewCatalogStore(cli)
	deps := engine.Deps{
		Client: cli, Reg: reg, Store: store,
		Cache: meta.NewCatalogCache(store), Exec: exec.New(cli), Guard: txguard.New(cli, reg),
	}
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	srv, err := newServerWithListener(deps, kidb.Bootstrap{
		Accounts: []kidb.Account{{User: "ro", Host: "%", Password: "", Role: "ro"}},
	}, l)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	time.Sleep(50 * time.Millisecond)
	defer srv.Close()

	db, err := sql.Open("mysql", fmt.Sprintf("ro:@tcp(%s)/kidb", l.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE TABLE t (id BIGINT PRIMARY KEY)"); err == nil {
		t.Fatal("ro 账号 DDL 必须报 1290")
	}
}
