package gateway

import (
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// TestClassifyMatchesTiDBParser 是 docs/12 §12.3 P5 的落地：
// 前置分类器与 TiDB parser 的语句类型判定对拍——
// parser 能解析的语句，分类器的 DDL 判定必须与 AST 顶层节点类型一致。
// 解析失败的语句不约束（路由到 DDL 路径后也会被正确报错）。
func TestClassifyMatchesTiDBParser(t *testing.T) {
	seeds := []string{
		"CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR(8)) COMMENT='kidb:{}'",
		"CREATE INDEX idx ON t (v)",
		"CREATE UNIQUE INDEX idx ON t (v)",
		"DROP TABLE t",
		"DROP INDEX idx ON t",
		"ALTER TABLE t ADD INDEX i (v)",
		"SELECT * FROM t WHERE a = 1",
		"SELECT a, COUNT(*) FROM t GROUP BY a",
		"INSERT INTO t VALUES (1, 'x')",
		"UPDATE t SET v = 'y' WHERE id = 1",
		"DELETE FROM t WHERE id = 1",
		"REPLACE INTO t VALUES (1)",
		"SHOW TABLES",
		"SET GLOBAL x = 1",
		"SELECT '/* not comment */', \"-- s\" FROM t",
	}
	prefixMutations := []string{"", " ", "\n\t", "-- lead\n", "# lead\n", "/* lead */ ", "/* multi\nline */ "}
	caseMutations := []func(string) string{
		func(s string) string { return s },
		strings.ToUpper,
		strings.ToLower,
	}

	rapid.Check(t, func(rt *rapid.T) {
		sql := rapid.SampledFrom(seeds).Draw(rt, "seed")
		pre := rapid.SampledFrom(prefixMutations).Draw(rt, "prefix")
		cf := rapid.SampledFrom(caseMutations).Draw(rt, "case")
		sql = pre + cf(sql)

		stmts, _, err := parser.New().Parse(sql, "", "")
		if err != nil || len(stmts) != 1 {
			return // 解析失败不约束（docs/02 §2.2：DDL 路径内会正确报错）
		}
		isDDL := isDDLStmt(stmts[0])
		got := Classify(sql) == RouteDDL
		if isDDL != got {
			rt.Fatalf("classify/parser 分歧: sql=%q parserDDL=%v classifyDDL=%v", sql, isDDL, got)
		}
	})
}

// isDDLStmt 报告 AST 顶层节点是否属于 KiDB DDL 白名单（docs/02 §2.4）。
func isDDLStmt(s ast.StmtNode) bool {
	switch s.(type) {
	case *ast.CreateTableStmt, *ast.CreateIndexStmt, *ast.DropTableStmt,
		*ast.DropIndexStmt, *ast.AlterTableStmt, *ast.TruncateTableStmt:
		return true
	}
	return false
}
