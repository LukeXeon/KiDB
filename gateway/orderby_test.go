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

// TestOrderByRangeColOrderedStreaming 范围索引列 ORDER BY 的全局有序性
// （docs/04 §4.1：ORDER BY num LIMIT k = 沿 score 有序桶端点 k 路归并）。
//
// 背景（gms replace_sort.go 分析器契约）：ORDER BY 列与索引表达式前缀匹配时
// gms 直接删除 Sort 节点（ASC 不咨询任何接口；DESC/全表仅查 OrderedIndex），
// 等价于约定"索引扫描必须按索引列升序产出"。KiDB 的范围桶按 slot 散布，
// 若按 slot 组顺序流式产出则全局无序——本测试钉死该契约。
func TestOrderByRangeColOrderedStreaming(t *testing.T) {
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
	execSQL("CREATE TABLE ob (uid BIGINT NOT NULL, age INT, PRIMARY KEY (uid)) COMMENT 'kidb:{}'")
	execSQL("CREATE INDEX idx_age ON ob (age)")
	require.Eventually(t, func() bool {
		rows, qerr := db.QueryContext(ctx, "SELECT uid FROM ob WHERE age >= 0")
		if qerr != nil {
			return false // Building 期查询被拦截（文档化语义）
		}
		rows.Close()
		return true
	}, 15*time.Second, 100*time.Millisecond)

	// 200 行，age 打散到多个 slot（重复值刻意存在：同分倾斜常态）
	for i := 1; i <= 200; i++ {
		execSQL(fmt.Sprintf("INSERT INTO ob (uid, age) VALUES (%d, %d)", i, (i*37)%50))
	}

	queryAges := func(q string) []int64 {
		t.Helper()
		rows, err := db.QueryContext(ctx, q)
		require.NoError(t, err, q)
		defer rows.Close()
		var ages []int64
		for rows.Next() {
			var uid, age int64
			require.NoError(t, rows.Scan(&uid, &age))
			ages = append(ages, age)
		}
		require.NoError(t, rows.Err())
		return ages
	}
	assertOrdered := func(ages []int64, desc bool) {
		t.Helper()
		require.Len(t, ages, 200, "全量行数")
		for i := 1; i < len(ages); i++ {
			if desc {
				require.GreaterOrEqual(t, ages[i-1], ages[i], "DESC 序在第 %d 行断裂: %v…", i, ages[max(0, i-3):min(len(ages), i+2)])
			} else {
				require.LessOrEqual(t, ages[i-1], ages[i], "ASC 序在第 %d 行断裂: %v…", i, ages[max(0, i-3):min(len(ages), i+2)])
			}
		}
	}

	// 带 WHERE 的 ASC/DESC（gms Case A：索引前缀匹配 → Sort 被删）
	assertOrdered(queryAges("SELECT uid, age FROM ob WHERE age >= 0 ORDER BY age ASC"), false)
	assertOrdered(queryAges("SELECT uid, age FROM ob WHERE age >= 0 ORDER BY age DESC"), true)

	// 无 WHERE 的 ORDER BY（gms Case B：全范围静态 lookup 替换表扫描）
	assertOrdered(queryAges("SELECT uid, age FROM ob ORDER BY age ASC"), false)
	assertOrdered(queryAges("SELECT uid, age FROM ob ORDER BY age DESC"), true)

	// top-k 精确性：LIMIT k 必须返回全局最小/最大的 k 行
	rows, err := db.QueryContext(ctx, "SELECT age FROM ob ORDER BY age ASC LIMIT 10")
	require.NoError(t, err)
	var top []int64
	for rows.Next() {
		var a int64
		require.NoError(t, rows.Scan(&a))
		top = append(top, a)
	}
	rows.Close()
	require.Len(t, top, 10)
	for i, a := range top {
		require.Equal(t, int64(i/4), a, "age=(i*37)%%50 分布下 top10 应为 0,0,0,0,1,1,1,1,2,2（实际 %v）", top)
	}

	// keyset 分页链（docs/04 §4.5：WHERE num > ? ORDER BY num LIMIT k 为最优路径）
	var chained []int64
	cursor := int64(-1)
	for {
		rows, err := db.QueryContext(ctx, "SELECT age FROM ob WHERE age > ? ORDER BY age LIMIT 7", cursor)
		require.NoError(t, err)
		var page []int64
		for rows.Next() {
			var a int64
			require.NoError(t, rows.Scan(&a))
			page = append(page, a)
		}
		rows.Close()
		if len(page) == 0 {
			break
		}
		chained = append(chained, page...)
		cursor = page[len(page)-1]
		require.LessOrEqual(t, len(chained), 400, "keyset 链必须收敛（去重后 50 个 distinct 值）")
		if len(page) < 7 && cursor == page[len(page)-1] {
			break
		}
	}
	for i := 1; i < len(chained); i++ {
		require.GreaterOrEqual(t, chained[i], chained[i-1], "keyset 链全局非降")
	}
}

// TestOrderByPKPointSet 主键点集 ORDER BY pk：gms Case A 对 PRIMARY 点查同样删 Sort
// （点范围互不重叠），exec 必须按 pk 值序产出。
func TestOrderByPKPointSet(t *testing.T) {
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
	execSQL("CREATE TABLE pkord (uid BIGINT NOT NULL, v INT, PRIMARY KEY (uid)) COMMENT 'kidb:{}'")
	for i := 1; i <= 30; i++ {
		execSQL(fmt.Sprintf("INSERT INTO pkord (uid, v) VALUES (%d, %d)", i, i))
	}

	// IN 乱序 + ASC：结果必须按 pk 升序
	rows, err := db.QueryContext(ctx, "SELECT uid FROM pkord WHERE uid IN (17, 3, 29, 8, 25, 1, 12) ORDER BY uid ASC")
	require.NoError(t, err)
	var got []int64
	for rows.Next() {
		var v int64
		require.NoError(t, rows.Scan(&v))
		got = append(got, v)
	}
	rows.Close()
	require.Equal(t, []int64{1, 3, 8, 12, 17, 25, 29}, got)

	// 主键范围 + ORDER BY pk：无有序结构，引擎层 sort 承载（正确即可）
	rows, err = db.QueryContext(ctx, "SELECT uid FROM pkord WHERE uid >= 20 ORDER BY uid DESC")
	require.NoError(t, err)
	got = got[:0]
	for rows.Next() {
		var v int64
		require.NoError(t, rows.Scan(&v))
		got = append(got, v)
	}
	rows.Close()
	require.Equal(t, []int64{30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20}, got)
}
