package controller

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"kidb/bucketmap"
	"kidb/exec"
	"kidb/meta"
	"kidb/testutil"
	"kidb/txguard"
)

// TestDDLJobResumable DDL 作业续作（docs/06 §6.3 + docs/12 §12.3 P6 形态）：
// 作业落库 → 分批推进（中途换执行器接管）→ 完成后索引可见且内容正确。
func TestDDLJobResumable(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	bm := bucketmap.New(cli, reg)
	g := txguard.New(cli, reg, bm)
	e := exec.New(cli, reg)
	e.SetBucketMap(bm)
	store := meta.NewCatalogStore(cli, reg)
	cache := meta.NewCatalogCache(store)
	ctx := context.Background()

	// 建表 + 300 行（索引创建前已有数据）
	tbl := &meta.TableDef{
		Name: "jobs",
		Columns: []meta.ColumnDef{
			{Name: "id", Type: meta.ColInt, NotNull: true},
			{Name: "age", Type: meta.ColInt},
		},
		PK: "id",
	}
	require.NoError(t, store.Save(ctx, tbl, 0))
	require.NoError(t, store.RegisterTable(ctx, "jobs"))
	for i := 1; i <= 300; i++ {
		_, err := g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: strconv.Itoa(i), Fields: map[string]string{"age": strconv.Itoa(i)}})
		require.NoError(t, err)
	}

	// 发起 CREATE INDEX 作业（Building 不可见语义）
	idx := &meta.IndexDef{ID: "idx_age", Columns: []string{"age"}, Kind: meta.IndexRange, Building: true}
	def, _ := store.Load(ctx, "jobs")
	def.Indexes = append(def.Indexes, *idx)
	require.NoError(t, store.Save(ctx, def, def.Ver))
	require.NoError(t, store.SetJob(ctx, "jobs", &meta.DDLJob{Type: "create_index", Index: idx, Cursor: 0}))
	cache.Invalidate()

	// 推进：小批次 + 中途"换执行器"（新 JobRunner 实例模拟宕机接管）
	jr1 := NewJobRunner(cli, store, cache, e, bm)
	jr1.slotsPerT = 4000              // 测试用小批
	jr1.tickBudget = time.Millisecond // 预算极小：一轮 tick 只跑一批（验证断点续作）
	require.NoError(t, jr1.Tick(ctx))
	job, err := store.GetJob(ctx, "jobs")
	require.NoError(t, err)
	require.NotNil(t, job, "第一批后作业未完成（游标落库）")
	require.Greater(t, job.Cursor, 0)

	// 接管：新实例从游标续作
	jr2 := NewJobRunner(cli, store, cache, e, bm)
	jr2.slotsPerT = 4000
	jr2.tickBudget = 500 * time.Millisecond
	for i := 0; i < 10; i++ {
		require.NoError(t, jr2.Tick(ctx))
		job, _ = store.GetJob(ctx, "jobs")
		if job == nil {
			break
		}
	}
	require.Nil(t, job, "作业应完成")

	// 完成后：索引可见（Building 清除）且内容完整正确
	def, err = store.Load(ctx, "jobs")
	require.NoError(t, err)
	require.False(t, def.Index("idx_age").Building)

	// 范围查询走新索引：age ∈ [100, 109] 应得 10 行
	rb := exec.RangeBound{Lo: 100, Hi: 109}
	s := e.Run(ctx, &exec.Request{
		Table: def, Kind: exec.RangeLookup, Index: def.Index("idx_age"),
		Ranges: []exec.RangeBound{rb},
		Pred:   &exec.Predicate{Column: "age", Ranges: []exec.RangeBound{rb}},
	})
	n := 0
	for {
		_, err := s.Next()
		if err != nil {
			break
		}
		n++
	}
	s.Close()
	require.Equal(t, 10, n, "回填后索引查询完整")
}
