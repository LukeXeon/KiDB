package ddl

import (
	"errors"
	"testing"

	"kidb"
	"kidb/meta"
)

func TestParseCreateTable(t *testing.T) {
	op, err := Parse(`CREATE TABLE sessions (
  uid BIGINT NOT NULL,
  token VARCHAR(64) NOT NULL,
  age INT,
  profile JSON,
  PRIMARY KEY (uid),
  INDEX idx_age (age),
  UNIQUE KEY uk_token (token)
) COMMENT 'kidb:{"default_ttl":86400}'`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if op.Kind != OpCreateTable {
		t.Fatalf("kind = %v", op.Kind)
	}
	d := op.Def
	if d.Name != "sessions" || d.PK != "uid" {
		t.Fatalf("def = %+v", d)
	}
	if d.DefaultTTL != 86400 {
		t.Fatalf("kidb 表选项未解析: %+v", d)
	}
	if len(d.Columns) != 4 || d.Columns[3].Type != meta.ColJSON {
		t.Fatalf("columns = %+v", d.Columns)
	}
	if len(d.Indexes) != 2 {
		t.Fatalf("indexes = %+v", d.Indexes)
	}
	age := d.Index("idx_age")
	if age == nil || age.Kind != meta.IndexRange { // INT → 范围索引（形态推导）
		t.Fatalf("idx_age = %+v", age)
	}
	tok := d.Index("uk_token")
	// 字符串唯一索引自动带字典序副本（docs/01 §1.0：无需 prefix_copy 声明）
	if tok == nil || tok.Kind != meta.IndexUnique || !tok.PrefixCopy {
		t.Fatalf("uk_token = %+v", tok)
	}
}

func TestParseCreateIndexWithCovering(t *testing.T) {
	op, err := Parse("CREATE INDEX idx ON t (a) COMMENT 'kidb:{\"covering\":[\"b\",\"c\"]}'")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if op.Kind != OpCreateIndex || op.Index.ID != "idx" || len(op.Index.Covering) != 2 {
		t.Fatalf("op = %+v", op)
	}
}

func TestParseRejects(t *testing.T) {
	cases := []string{
		"CREATE TABLE t (id BIGINT)",                                         // 无主键
		"CREATE TABLE t (_ver BIGINT, PRIMARY KEY (_ver))",                   // 保留列
		"CREATE TABLE t (id SET('a'), PRIMARY KEY (id))",                     // 类型白名单外
		"CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b))",                  // 复合主键
		"CREATE TABLE t (id BIGINT PRIMARY KEY, INDEX i (id), INDEX i (id))", // 索引重名
		"TRUNCATE TABLE t",               // 无界操作
		"ALTER TABLE t ADD COLUMN c INT", // v5.0 仅 ADD/DROP INDEX
		"CREATE TABLE t (id BIGINT PRIMARY KEY, FOREIGN KEY (id) REFERENCES x (y))", // 外键
	}
	for _, sql := range cases {
		op, err := Parse(sql)
		if err == nil {
			t.Fatalf("%q 应被拒绝, got %+v", sql, op)
		}
		if !errors.Is(err, kidb.ErrUnsupported) && !isPlainErr(err) {
			t.Fatalf("%q 错误类型意外: %v", sql, err)
		}
	}
}

func isPlainErr(err error) bool { return err != nil }

func TestParseAsyncUniqueMutualExclusion(t *testing.T) {
	_, err := Parse("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(8), UNIQUE KEY uk (v) COMMENT 'kidb:{\"async\":true}')")
	if err == nil {
		t.Fatal("async + unique 互斥校验缺失（docs/02 §2.4）")
	}
}

func TestParseDrop(t *testing.T) {
	op, err := Parse("DROP TABLE sessions")
	if err != nil || op.Kind != OpDropTable || op.Table != "sessions" {
		t.Fatalf("drop table: %v %+v", err, op)
	}
	op, err = Parse("DROP INDEX idx ON sessions")
	if err != nil || op.Kind != OpDropIndex || op.IndexID != "idx" || op.Table != "sessions" {
		t.Fatalf("drop index: %v %+v", err, op)
	}
	op, err = Parse("ALTER TABLE sessions DROP INDEX idx")
	if err != nil || op.Kind != OpDropIndex {
		t.Fatalf("alter drop index: %v %+v", err, op)
	}
}
