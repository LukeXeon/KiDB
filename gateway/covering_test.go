package gateway

import (
	"context"
	"database/sql"
	"fmt"
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

// wireCounter 统计内核发出的 Redis 命令（覆盖路径零回表的端到端证据）。
// 回表计数只算行 key（d: 前缀）——Catalog lease 刷新等元数据 HGETALL 不算。
type wireCounter struct {
	kidb.KvClient
	mu         sync.Mutex
	counts     map[string]int
	rowHGETALL int // 行 key HGETALL（回表）
	rowHMGET   int // 行 key HMGET（投影下推回表）
}

func (w *wireCounter) Do(ctx context.Context, cmd string, args ...any) (any, error) {
	w.mu.Lock()
	w.counts[cmd]++
	w.trackFetch(cmd, args)
	w.mu.Unlock()
	return w.KvClient.Do(ctx, cmd, args...)
}

func (w *wireCounter) Pipeline(ctx context.Context, cmds []kidb.Cmd) ([]any, error) {
	w.mu.Lock()
	for _, c := range cmds {
		w.counts[c.Name]++
		w.trackFetch(c.Name, c.Args)
	}
	w.mu.Unlock()
	return w.KvClient.Pipeline(ctx, cmds)
}

func (w *wireCounter) trackFetch(cmd string, args []any) {
	if len(args) == 0 {
		return
	}
	k, ok := args[0].(string)
	if !ok || !strings.HasPrefix(k, "d:") {
		return
	}
	switch cmd {
	case "HGETALL":
		w.rowHGETALL++
	case "HMGET":
		w.rowHMGET++
	}
}

func (w *wireCounter) count(name string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.counts[name]
}

func (w *wireCounter) rowFetches() (hgetall, hmget int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rowHGETALL, w.rowHMGET
}

func (w *wireCounter) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.counts = map[string]int{}
	w.rowHGETALL, w.rowHMGET = 0, 0
}

// TestCoveringWire 覆盖索引读路径的端到端验证（docs/03 §3.5）：
// 经 gms 全链路（pruneTables 投影下推 → ITA → translate 覆盖判定），
// 命中覆盖的投影查询零回表（无 HGETALL/HMGET），结果精确。
func TestCoveringWire(t *testing.T) {
	cli, reg, _ := redistest.New(t)
	wc := &wireCounter{KvClient: cli, counts: map[string]int{}}

	store := meta.NewCatalogStore(cli, reg)
	deps := engine.Deps{
		Client: wc, // 计数包装进内核依赖面
		Reg:    reg,
		Store:  store,
		Cache:  meta.NewCatalogCache(store),
		Exec:   exec.New(cli, reg), // exec 直接用裸客户端——计数经 Guard/引擎侧不可见……
		Guard:  txguard.New(cli, reg, nil),
	}
	deps.Exec = exec.New(wc, reg) // exec 也走计数客户端

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv, err := newServerWithListener(deps, kidb.Bootstrap{
		Accounts: []kidb.Account{{User: "root", Host: "%", Password: "", Role: "rw"}},
	}, l)
	require.NoError(t, err)
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
	execSQL("CREATE TABLE cov (uid BIGINT NOT NULL, city VARCHAR(32) NOT NULL, age INT, note VARCHAR(32), PRIMARY KEY (uid)) COMMENT 'kidb:{}'")
	execSQL("CREATE INDEX idx_age ON cov (age) COMMENT 'kidb:{\"covering\":[\"city\"]}'")
	require.Eventually(t, func() bool {
		rows, qerr := db.QueryContext(ctx, "SELECT uid FROM cov WHERE age >= 0")
		if qerr != nil {
			return false
		}
		rows.Close()
		return true
	}, 15*time.Second, 100*time.Millisecond, "等索引回填完成")
	for i := 1; i <= 60; i++ {
		execSQL(fmt.Sprintf("INSERT INTO cov (uid, city, age, note) VALUES (%d, 'city%d', %d, 'n%d')", i, i%7, 20+i%40, i))
	}

	// 覆盖命中：投影 {city,age} ⊆ {age(索引列), city(覆盖), uid(pk)}
	wc.reset()
	rows, err := db.QueryContext(ctx, "SELECT city, age FROM cov WHERE age >= 25 ORDER BY age DESC LIMIT 5")
	require.NoError(t, err)
	var got [][2]any
	for rows.Next() {
		var city string
		var age int64
		require.NoError(t, rows.Scan(&city, &age))
		got = append(got, [2]any{city, age})
	}
	rows.Close()
	require.Len(t, got, 5)
	for i := 1; i < len(got); i++ {
		require.GreaterOrEqual(t, got[i-1][1].(int64), got[i][1].(int64), "DESC 序")
	}
	require.Equal(t, int64(59), got[0][1]) // age 最大 = 20+39=59
	hg, hm := wc.rowFetches()
	require.Equal(t, 0, hg+hm, "覆盖命中零回表（行 key HGETALL=%d HMGET=%d）", hg, hm)
	require.Positive(t, wc.count("ZSCORE"), "活性校验必经 exp ZSCORE")

	// 非覆盖投影（含 note）：投影下推 HMGET（不取 HGETALL），结果同样精确
	wc.reset()
	rows, err = db.QueryContext(ctx, "SELECT note FROM cov WHERE age = 33")
	require.NoError(t, err)
	var notes []string
	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		notes = append(notes, n)
	}
	rows.Close()
	require.NotEmpty(t, notes)
	hg, hm = wc.rowFetches()
	require.Equal(t, 0, hg, "投影查询应走 HMGET 子集（行 key HGETALL=%d）", hg)
	require.Positive(t, hm, "投影查询应 HMGET（行 key HMGET=%d）", hm)

	// SELECT * 全列：HGETALL（投影=全集时 HGETALL 更省，行为正确即可）
	rows, err = db.QueryContext(ctx, "SELECT * FROM cov WHERE age = 34")
	require.NoError(t, err)
	cnt := 0
	for rows.Next() {
		cnt++
	}
	rows.Close()
	require.Positive(t, cnt)
}
