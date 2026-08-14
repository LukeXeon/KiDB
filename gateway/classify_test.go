package gateway

import "testing"

func TestClassify(t *testing.T) {
	cases := []struct {
		sql  string
		want Route
	}{
		{"CREATE TABLE t (id BIGINT PRIMARY KEY)", RouteDDL},
		{"create table t (id bigint)", RouteDDL}, // 大小写不敏感
		{"  -- 建表\nCREATE TABLE t (id BIGINT)", RouteDDL},
		{"/* 注释 */ CREATE INDEX i ON t (a)", RouteDDL},
		{"CREATE UNIQUE INDEX i ON t (a)", RouteDDL},
		{"CREATE FULLTEXT INDEX i ON t (a)", RouteDDL}, // DDL 路径内报错
		{"# 注释\nDROP TABLE t", RouteDDL},
		{"DROP INDEX i ON t", RouteDDL},
		{"ALTER TABLE t ADD INDEX i (a)", RouteDDL},
		{"TRUNCATE TABLE t", RouteDDL}, // DDL 路径内报错（docs/02 §2.4）
		{"SELECT 1", RouteEngine},
		{"/*+ FULLSCAN */ SELECT * FROM t", RouteEngine},
		{"SELECT '/* 不是注释 */' FROM t", RouteEngine},
		{"SELECT '--' , '#'", RouteEngine},
		{"INSERT INTO t VALUES ('a--b', 'c#d', 'e/*f*/g')", RouteEngine},
		{"SELECT `odd--name` FROM t", RouteEngine},
		{"WITH cte AS (SELECT 1) SELECT * FROM cte", RouteEngine},
		{"SHOW TABLES", RouteEngine},
		{"SET GLOBAL bucket_split_members = 60000", RouteEngine},
		{"EXPLAIN SELECT * FROM t", RouteEngine},
		{"USE db1", RouteEngine},
		{"", RouteEngine},
		{"  -- 只有注释", RouteEngine},
		{"SELECT 'it\\'s -- tricky'", RouteEngine},
	}
	for _, c := range cases {
		if got := Classify(c.sql); got != c.want {
			t.Errorf("Classify(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}
