package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"

	"kidb/bucketmap"
	"kidb/engine"
	"kidb/exec"
	"kidb/keycodec"
	"kidb/kv"
	"kidb/meta"
	"kidb/script"
	"kidb/testutil"
	"kidb/tuning"
	"kidb/txguard"
)

// dropjob_test.go：DROP TABLE 大表后台清理作业（docs/06 §6.3）——
// 作业化分流、Catalog 即刻删除、断点续作（换实例接管）、全量清理。

// TestDropTableJob 大表 DROP 作业化全流程。
func TestDropTableJob(t *testing.T) {
	cli, reg, m := testutil.New(t)
	store := meta.NewCatalogStore(cli, reg)
	cache := meta.NewCatalogCache(store)
	bm := bucketmap.New(cli, reg)
	ex := exec.New(cli, reg)
	guard := txguard.New(cli, reg, nil)
	ctx := context.Background()

	tbl := &meta.TableDef{
		Name: "big_t",
		Columns: []meta.ColumnDef{
			{Name: "uid", Type: meta.ColInt, TypeText: "bigint", NotNull: true},
			{Name: "city", Type: meta.ColString, TypeText: "varchar(32)"},
			{Name: "age", Type: meta.ColInt, TypeText: "int"},
			{Name: "email", Type: meta.ColString, TypeText: "varchar(64)"},
		},
		PK: "uid",
		Indexes: []meta.IndexDef{
			{ID: "idx_city", Columns: []string{"city"}, Kind: meta.IndexEq, PrefixCopy: true},
			{ID: "idx_age", Columns: []string{"age"}, Kind: meta.IndexRange},
			{ID: "uk_email", Columns: []string{"email"}, Kind: meta.IndexUnique},
		},
	}
	require.NoError(t, store.Save(ctx, tbl, 0))
	require.NoError(t, store.RegisterTable(ctx, tbl.Name))

	p := testutil.NewProbe(t, cli)
	for i := 1; i <= 50; i++ {
		_, err := guard.WriteRow(ctx, txguard.WriteReq{
			Table: tbl, PK: fmt.Sprint(i),
			Fields: map[string]string{"city": fmt.Sprintf("c%d", i%3), "age": fmt.Sprint(20 + i), "email": fmt.Sprintf("u%d@x.com", i)},
		})
		require.NoError(t, err)
	}
	// 过期行（物理过期后 Sweeper 未跑 → 桶残留/回执/预约的混合场景）
	for i := 51; i <= 60; i++ {
		_, err := guard.WriteRow(ctx, txguard.WriteReq{
			Table: tbl, PK: fmt.Sprint(i),
			Fields: map[string]string{"city": "ghost", "age": "99", "email": fmt.Sprintf("g%d@x.com", i)},
			TTL:    50 * time.Millisecond,
		})
		require.NoError(t, err)
	}
	m.FastForward(time.Minute)

	// 强制作业化 + 缩小批推进（模拟多轮续作）
	tuning.OverrideForTest(t, func(tn *tuning.Tuning) {
		tn.Controller.DropSyncMaxRows = 0
		tn.Controller.JobSlotsPerTick = 2048
	})

	// DROP（引擎层 TableDropper 路径）
	deps := engine.Deps{Client: cli, Reg: reg, Store: store, Cache: cache, Exec: ex, Guard: guard}
	_, pro, err := engine.Build(deps)
	require.NoError(t, err)
	sqlCtx := sql.NewContext(ctx)
	dbase, err := pro.Database(sqlCtx, "kidb")
	require.NoError(t, err)
	require.NoError(t, dbase.(sql.TableDropper).DropTable(sqlCtx, tbl.Name))

	// Catalog 即刻删除：Load nil、注册表移除、作业登记在案
	def, err := store.Load(ctx, tbl.Name)
	require.NoError(t, err)
	require.Nil(t, def)
	names, err := store.ListTables(ctx)
	require.NoError(t, err)
	require.NotContains(t, names, tbl.Name)
	jobs, err := store.ListDropJobs(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, tbl.Name, jobs[0].Table)
	require.NotNil(t, jobs[0].Def)

	// 行还在（未清理）；前 4 轮由一个实例推进（模拟中途宕机换实例接管）
	jr1 := NewJobRunner(cli, reg, store, cache, ex, bm, guard)
	for i := 0; i < 4; i++ {
		require.NoError(t, jr1.Tick(ctx))
	}
	jobs, err = store.ListDropJobs(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1, "4 轮（2048 slot/轮）后作业应在推进中（游标 >0 未完成）")
	require.Positive(t, jobs[0].Cursor)

	// 换实例接管（巡检接管语义）跑到完成
	jr2 := NewJobRunner(cli, reg, store, cache, ex, bm, guard)
	for i := 0; i < 8; i++ {
		require.NoError(t, jr2.Tick(ctx))
	}
	jobs, err = store.ListDropJobs(ctx)
	require.NoError(t, err)
	require.Empty(t, jobs, "作业应已完成注销")

	// 全量清理断言：行/等值桶/范围桶/字典序副本/登记册/预约/索引级残留
	for i := 1; i <= 60; i++ {
		require.False(t, p.Exists(keycodec.RowKey(tbl.Name, fmt.Sprint(i))), "行 %d 应已删除", i)
	}
	for slot := 0; slot < keycodec.NumSlots; slot += 4096 { // 抽查 slot
		st := uint16(slot)
		require.Empty(t, p.Get(keycodec.ExpKeyN(tbl.Name, st, 0, 1)), "登记册应已清")
		for _, idx := range []string{"idx_city", "idx_age"} {
			require.False(t, p.Exists(keycodec.BucketMapSlotKey(tbl.Name, idx, st)), "bm 分片应已清")
		}
	}
	require.False(t, p.Exists(keycodec.UniqueKey(tbl.Name, "uk_email", "u1@x.com")), "唯一预约应已释放")
	require.False(t, p.Exists(keycodec.HLLKey(tbl.Name, "idx_age")), "HLL 应已清")
	require.False(t, p.Exists(keycodec.BucketMapHotKey(tbl.Name, "idx_city")), "热值注册表应已清")
	require.Empty(t, p.ZScore(keycodec.EqBucketKey(tbl.Name, "idx_city", "c1", keycodec.Slot(keycodec.RowKey(tbl.Name, "1")), 0), "1"), "等值桶成员应已清")
	require.Empty(t, p.ZScore(keycodec.RangeBucketKey(tbl.Name, "idx_age", keycodec.Slot(keycodec.RowKey(tbl.Name, "1")), 0), "1"), "范围桶成员应已清")
}

// TestDropTableSmallSync 小表维持同步路径（阈值内无作业登记）。
func TestDropTableSmallSync(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	store := meta.NewCatalogStore(cli, reg)
	cache := meta.NewCatalogCache(store)
	ex := exec.New(cli, reg)
	guard := txguard.New(cli, reg, nil)
	ctx := context.Background()

	tbl := &meta.TableDef{
		Name:    "small_t",
		Columns: []meta.ColumnDef{{Name: "uid", Type: meta.ColInt, TypeText: "bigint", NotNull: true}},
		PK:      "uid",
	}
	require.NoError(t, store.Save(ctx, tbl, 0))
	require.NoError(t, store.RegisterTable(ctx, tbl.Name))
	for i := 1; i <= 3; i++ {
		_, err := guard.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: fmt.Sprint(i), Fields: map[string]string{}})
		require.NoError(t, err)
	}

	deps := engine.Deps{Client: cli, Reg: reg, Store: store, Cache: cache, Exec: ex, Guard: guard}
	_, pro, err := engine.Build(deps)
	require.NoError(t, err)
	sqlCtx := sql.NewContext(ctx)
	dbase, err := pro.Database(sqlCtx, "kidb")
	require.NoError(t, err)
	require.NoError(t, dbase.(sql.TableDropper).DropTable(sqlCtx, tbl.Name))

	jobs, err := store.ListDropJobs(ctx)
	require.NoError(t, err)
	require.Empty(t, jobs, "小表同步清理，不得登记作业")
	require.False(t, testutil.NewProbe(t, cli).Exists(keycodec.RowKey(tbl.Name, "1")))
}

// TestDropJobBlocksRecreate DROP 清理作业在途 → 同名 CREATE 拒绝；完成 → 放行
// 且旧代行不可见（review 实证缺口：撞名重建曾把旧作业永久卡死 + 幽灵行可见）。
func TestDropJobBlocksRecreate(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	store := meta.NewCatalogStore(cli, reg)
	cache := meta.NewCatalogCache(store)
	ex := exec.New(cli, reg)
	guard := txguard.New(cli, reg, nil)
	ctx := context.Background()

	tbl := &meta.TableDef{
		Name:    "dr",
		Columns: []meta.ColumnDef{{Name: "uid", Type: meta.ColInt, TypeText: "bigint", NotNull: true}},
		PK:      "uid",
	}
	require.NoError(t, store.Save(ctx, tbl, 0))
	require.NoError(t, store.RegisterTable(ctx, tbl.Name))
	_, err := guard.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: "1", Fields: map[string]string{}})
	require.NoError(t, err)

	tuning.OverrideForTest(t, func(tn *tuning.Tuning) {
		tn.Controller.DropSyncMaxRows = 0
		tn.Controller.JobSlotsPerTick = keycodec.NumSlots
	})

	deps := engine.Deps{Client: cli, Reg: reg, Store: store, Cache: cache, Exec: ex, Guard: guard}
	_, pro, err := engine.Build(deps)
	require.NoError(t, err)
	sqlCtx := sql.NewContext(ctx)
	dbase, err := pro.Database(sqlCtx, "kidb")
	require.NoError(t, err)
	require.NoError(t, dbase.(sql.TableDropper).DropTable(sqlCtx, "dr"))

	// 作业在途：同名重建拒绝
	sch := sql.NewPrimaryKeySchema(sql.Schema{{Name: "uid", Type: types.Int64, Nullable: false}}, 0)
	err = dbase.(sql.TableCreator).CreateTable(sqlCtx, "dr", sch, sql.Collation_Default, "kidb:{}")
	require.Error(t, err, "DROP 作业在途的同名重建必须拒绝")

	// 作业跑完 → 放行且旧行不可见
	jr := NewJobRunner(cli, reg, store, cache, ex, bm2(cli, reg), guard)
	require.NoError(t, jr.Tick(ctx))
	jobs, err := store.ListDropJobs(ctx)
	require.NoError(t, err)
	require.Empty(t, jobs)
	require.NoError(t, dbase.(sql.TableCreator).CreateTable(sqlCtx, "dr", sch, sql.Collation_Default, "kidb:{}"))
	require.False(t, testutil.NewProbe(t, cli).Exists(keycodec.RowKey("dr", "1")), "旧代行必须已被清理")
}

func bm2(cli kv.Client, reg *script.Registry) *bucketmap.Store { return bucketmap.New(cli, reg) }
