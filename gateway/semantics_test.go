package gateway

// semantics_test.go：DML/DDL 语义回归（2026-08-16 架构 review 的实证探针转正）。
// 每条对应一个 review 坐实过的缺陷形态；详情见 CHANGELOG v6.x 修复批次。

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"kidb/keycodec"
	"kidb/nearcache"
)

func semDB(t *testing.T) (*sql.DB, context.Context, func()) {
	t.Helper()
	dsn, _, _, cleanup := newTestServer(t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	return db, ctx, func() { cancel(); db.Close(); cleanup() }
}

func mustExec(t *testing.T, db *sql.DB, ctx context.Context, q string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// REPLACE = upsert 覆盖（gms 编排 delete+replace；回归钉死）。
func TestReplaceSemantics(t *testing.T) {
	db, ctx, done := semDB(t)
	defer done()
	mustExec(t, db, ctx, "CREATE TABLE rt (id BIGINT NOT NULL, v VARCHAR(32), PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	mustExec(t, db, ctx, "INSERT INTO rt (id, v) VALUES (1, 'old')")
	mustExec(t, db, ctx, "REPLACE INTO rt (id, v) VALUES (1, 'new')")
	var v string
	if err := db.QueryRowContext(ctx, "SELECT v FROM rt WHERE id = 1").Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != "new" {
		t.Fatalf("REPLACE 后 v=%q，期待 new", v)
	}
}

// UPDATE 主键变更撞活行必须 1062（曾静默覆盖受害者行——数据丢失形态）。
func TestUpdatePKConflictRejected(t *testing.T) {
	db, ctx, done := semDB(t)
	defer done()
	mustExec(t, db, ctx, "CREATE TABLE upt (id BIGINT NOT NULL, v VARCHAR(32), PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	mustExec(t, db, ctx, "INSERT INTO upt (id, v) VALUES (1, 'a'), (2, 'b')")
	if _, err := db.ExecContext(ctx, "UPDATE upt SET id = 2 WHERE id = 1"); err == nil {
		t.Fatal("UPDATE pk 撞活行必须报 1062")
	}
	var v string
	if err := db.QueryRowContext(ctx, "SELECT v FROM upt WHERE id = 2").Scan(&v); err != nil || v != "b" {
		t.Fatalf("受害者行必须原样：v=%q err=%v", v, err)
	}
}

// UPDATE/ODKU 置 NULL 必须生效（write_row.lua v6 撤字段面）。
func TestUpdateSetNull(t *testing.T) {
	db, ctx, done := semDB(t)
	defer done()
	mustExec(t, db, ctx, "CREATE TABLE un (id BIGINT NOT NULL, city VARCHAR(32), PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	mustExec(t, db, ctx, "INSERT INTO un (id, city) VALUES (1, 'shanghai')")
	mustExec(t, db, ctx, "UPDATE un SET city = NULL WHERE id = 1")
	var city sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT city FROM un WHERE id = 1").Scan(&city); err != nil {
		t.Fatal(err)
	}
	if city.Valid {
		t.Fatalf("UPDATE 置 NULL 未生效：city=%q", city.String)
	}
	mustExec(t, db, ctx, "INSERT INTO un (id, city) VALUES (1, 'beijing') ON DUPLICATE KEY UPDATE city = NULL")
	city = sql.NullString{}
	if err := db.QueryRowContext(ctx, "SELECT city FROM un WHERE id = 1").Scan(&city); err != nil {
		t.Fatal(err)
	}
	if city.Valid {
		t.Fatalf("ODKU 置 NULL 未生效：city=%q", city.String)
	}
}

// DATETIME(6) 必须 DDL 拒绝（存 Unix 秒——放行即写入静默截断）。
func TestDatetimePrecisionRejected(t *testing.T) {
	db, ctx, done := semDB(t)
	defer done()
	if _, err := db.ExecContext(ctx, "CREATE TABLE dt6 (id BIGINT NOT NULL, ts DATETIME(6), PRIMARY KEY (id)) COMMENT 'kidb:{}'"); err == nil {
		t.Fatal("datetime(6) 建表必须拒绝")
	}
}

// 标识符字符集白名单（key 布局纪律：分隔符/hash tag 语义防腐）。
func TestIdentifierCharset(t *testing.T) {
	db, ctx, done := semDB(t)
	defer done()
	if _, err := db.ExecContext(ctx, "CREATE TABLE `a:b` (id BIGINT NOT NULL, PRIMARY KEY (id)) COMMENT 'kidb:{}'"); err == nil {
		t.Fatal("含冒号表名必须拒绝")
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE ok_name2 (id BIGINT NOT NULL, `bad col` INT, PRIMARY KEY (id)) COMMENT 'kidb:{}'"); err == nil {
		t.Fatal("含空格列名必须拒绝")
	}
	mustExec(t, db, ctx, "CREATE TABLE ok_name2 (id BIGINT NOT NULL, good_col INT, PRIMARY KEY (id)) COMMENT 'kidb:{}'")
}

// SELECT * 遇候选集死行：死行滤除、活行照常（PTTL 同 pipeline 消费纪律）。
func TestSelectStarWithDeadRow(t *testing.T) {
	dsn, _, m, cleanup := newTestServer(t)
	defer cleanup()
	db, _ := sql.Open("mysql", dsn)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	mustExec(t, db, ctx, "CREATE TABLE td (id BIGINT NOT NULL, city VARCHAR(32), PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	mustExec(t, db, ctx, "CREATE INDEX idx_city ON td (city)")
	mustExec(t, db, ctx, "INSERT INTO td (id, city, _ttl) VALUES (1, 'x', 1)")
	mustExec(t, db, ctx, "INSERT INTO td (id, city) VALUES (2, 'x')")
	m.FastForward(3 * time.Second) // 行 1 物理过期（残留未清扫）

	var ids []int64
	rows, err := db.QueryContext(ctx, "SELECT * FROM td WHERE city = 'x'")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var city sql.NullString
		var ttl sql.NullInt64
		if err := rows.Scan(&id, &city, &ttl); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 1 || ids[0] != 2 {
		t.Fatalf("返回 %v，期待恰好 [2]", ids)
	}
}

// 投影子集（HMGET 路径）遇死行同样滤除（_ver 活性哨兵）。
func TestProjectionWithDeadRow(t *testing.T) {
	dsn, _, m, cleanup := newTestServer(t)
	defer cleanup()
	db, _ := sql.Open("mysql", dsn)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	mustExec(t, db, ctx, "CREATE TABLE tp (id BIGINT NOT NULL, city VARCHAR(32), age INT, PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	mustExec(t, db, ctx, "CREATE INDEX idx_city ON tp (city)")
	mustExec(t, db, ctx, "INSERT INTO tp (id, city, age, _ttl) VALUES (1, 'x', 20, 1)")
	mustExec(t, db, ctx, "INSERT INTO tp (id, city, age) VALUES (2, 'x', 30)")
	m.FastForward(3 * time.Second)

	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM (SELECT age FROM tp WHERE city='x') q").Scan(&n); err == nil && n == 1 {
		return // 派生表形态正常
	}
	// 直查投影形态
	rows, err := db.QueryContext(ctx, "SELECT age FROM tp WHERE city = 'x'")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []int
	for rows.Next() {
		var a int
		if err := rows.Scan(&a); err != nil {
			t.Fatal(err)
		}
		got = append(got, a)
	}
	if len(got) != 1 || got[0] != 30 {
		t.Fatalf("投影路径返回 %v，期待 [30]", got)
	}
}

// 范围索引重叠 OR 区间 + ORDER BY：去重且有序（gms 删 Sort 契约）。
func TestOverlappingRangesOrdered(t *testing.T) {
	db, ctx, done := semDB(t)
	defer done()
	mustExec(t, db, ctx, "CREATE TABLE ov (id BIGINT NOT NULL, age INT, PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	mustExec(t, db, ctx, "INSERT INTO ov (id, age) VALUES (1, 15), (2, 25), (3, 35)")
	mustExec(t, db, ctx, "CREATE INDEX idx_age ON ov (age)")
	rows, err := db.QueryContext(ctx, "SELECT id FROM ov WHERE age > 10 OR age > 20 ORDER BY age")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if fmt.Sprint(ids) != "[1 2 3]" {
		t.Fatalf("重叠 OR 区间 ORDER BY 产出 %v，期待 [1 2 3]", ids)
	}
}

// L1 谓词指纹长度前缀编码（分隔符注入串缓存形态回归）。
func TestL1FingerprintNoCollision(t *testing.T) {
	dsn, deps, _, cleanup := newTestServer(t)
	defer cleanup()
	deps.Exec.SetNearCache(nearcache.NewSharded[[]string](1000, 3*time.Second))
	db, _ := sql.Open("mysql", dsn)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	mustExec(t, db, ctx, "CREATE TABLE fp (id BIGINT NOT NULL, c VARCHAR(64), PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	mustExec(t, db, ctx, "CREATE INDEX idx_c ON fp (c)")
	mustExec(t, db, ctx, "INSERT INTO fp (id, c) VALUES (1, 'a,b'), (2, 'a')")

	var id int64
	if err := db.QueryRowContext(ctx, "SELECT id FROM fp WHERE c IN ('a,b','c')").Scan(&id); err != nil {
		t.Fatal(err)
	}
	var got int64
	if err := db.QueryRowContext(ctx, "SELECT id FROM fp WHERE c IN ('a','b,c')").Scan(&got); err != nil {
		t.Fatalf("q2 应命中 id=2: %v", err)
	}
	if got != 2 {
		t.Fatalf("q2 返回 id=%d，期待 2", got)
	}
}

// GRANT 在边界外（gms 自拒——钉死当前行为形态）。
func TestGrantRejected(t *testing.T) {
	db, ctx, done := semDB(t)
	defer done()
	if _, err := db.ExecContext(ctx, "GRANT SELECT ON *.* TO 'x'@'%'"); err == nil {
		t.Fatal("GRANT 必须报错")
	}
}

// SET autocommit=0 显式拒绝（无事务定位下的数据谎言形态回归）。
func TestAutocommitOffRejected(t *testing.T) {
	db, ctx, done := semDB(t)
	defer done()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET autocommit=0"); err == nil {
		t.Fatal("SET autocommit=0 必须拒绝")
	}
	// autocommit=1 照常
	if _, err := conn.ExecContext(ctx, "SET autocommit=1"); err != nil {
		t.Fatalf("SET autocommit=1 不应报错: %v", err)
	}
	mustExec(t, db, ctx, "CREATE TABLE ac2 (id BIGINT NOT NULL, PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	mustExec(t, db, ctx, "INSERT INTO ac2 (id) VALUES (1)")
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ac2").Scan(&n); err != nil || n != 1 {
		t.Fatalf("读回 n=%d err=%v", n, err)
	}
}

// 异步索引：含空格/中文的值正常可见（url.QueryEscape 可逆日志形态回归）。
func TestAsyncIndexEscapedValues(t *testing.T) {
	dsn, _, _, cleanup := newTestServer(t)
	defer cleanup()
	db, _ := sql.Open("mysql", dsn)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	mustExec(t, db, ctx, "CREATE TABLE asy (id BIGINT NOT NULL, c VARCHAR(64), PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	mustExec(t, db, ctx, "CREATE INDEX idx_c ON asy (c) COMMENT 'kidb:{\"async\":true}'")
	mustExec(t, db, ctx, "INSERT INTO asy (id, c) VALUES (1, 'hello world'), (2, '上海')")
	for i := 0; i < 50; i++ {
		var n int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM asy WHERE c = 'hello world'").Scan(&n); err == nil && n == 1 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, v := range []string{"hello world", "上海"} {
		var n int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM asy WHERE c = ?", v).Scan(&n); err != nil || n != 1 {
			t.Fatalf("异步索引值 %q 不可见: n=%d err=%v", v, n, err)
		}
	}
}

// 唯一索引存量面：存量值预约回填（撞库 1062）+ 存量重复值拒建。
func TestUniqueIndexExistingData(t *testing.T) {
	dsn, deps, _, cleanup := newTestServer(t)
	defer cleanup()
	db, _ := sql.Open("mysql", dsn)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	mustExec(t, db, ctx, "CREATE TABLE ub (id BIGINT NOT NULL, email VARCHAR(64), PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	mustExec(t, db, ctx, "INSERT INTO ub (id, email) VALUES (1, 'a@x.com')")
	mustExec(t, db, ctx, "CREATE UNIQUE INDEX uk_email ON ub (email)")
	for i := 0; i < 100; i++ {
		res, _ := deps.Client.Do(ctx, "EXISTS", keycodec.UniqueKey("ub", "uk_email", "a@x.com"))
		if fmt.Sprint(res) == "1" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO ub (id, email) VALUES (2, 'a@x.com')"); err == nil {
		t.Fatal("唯一索引对存量值必须生效（1062）")
	} else if !strings.Contains(err.Error(), "1062") && !strings.Contains(strings.ToLower(err.Error()), "duplicate") {
		t.Fatalf("期待 1062 类错误，实际: %v", err)
	}

	mustExec(t, db, ctx, "CREATE TABLE ub2 (id BIGINT NOT NULL, email VARCHAR(64), PRIMARY KEY (id)) COMMENT 'kidb:{}'")
	mustExec(t, db, ctx, "INSERT INTO ub2 (id, email) VALUES (1, 'd@x.com'), (2, 'd@x.com')")
	if _, err := db.ExecContext(ctx, "CREATE UNIQUE INDEX uk_email2 ON ub2 (email)"); err == nil {
		t.Fatal("存量重复值上 CREATE UNIQUE INDEX 必须拒绝")
	}
}
