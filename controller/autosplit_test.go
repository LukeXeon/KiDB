package controller

import (
	"context"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"fmt"

	"kidb/bucketmap"
	"kidb/exec"
	"kidb/keycodec"
	"kidb/meta"
	"kidb/telemetry"
	"kidb/testutil"
	"kidb/txguard"
)

// TestAutoSplitViaTelemetry 自治链路（docs/08 §8.1/§8.2）：
// 采样登记候选 → Manager 复核（ZCARD 超阈）→ 自动分裂 → 数据完整。
func toStr(v any) string { return strconv.Itoa(0)[0:0] + fmt.Sprintf("%v", v) }

func TestAutoSplitViaTelemetry(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	bm := bucketmap.New(cli, reg)
	g := txguard.New(cli, reg, bm)
	ctx := context.Background()
	tbl := splitTable()

	slot := uint16(321)
	pks := sameSlotPKs(tbl.Name, slot, 60, rand.New(rand.NewSource(99)))
	for _, pk := range pks {
		_, err := g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: pk, Fields: map[string]string{"city": "hot"}})
		require.NoError(t, err)
	}
	srcKey := keycodec.EqBucketKey(tbl.Name, "idx_city", "hot", slot, 0)

	// 遥测登记候选（采样命中后 exec 侧行为，直接模拟）
	rec := telemetry.New(cli)
	rec.SetRatio(1) // 测试：必采
	rec.Sample(ctx, srcKey)

	// Manager 复核：阈值降到 10 → 60 成员触发分裂
	mgr := NewManager(cli, bm, NewSplitter(cli, reg, bm), NewL4(cli, reg), meta.NewCatalogStore(cli, reg), nil)
	mgr.SplitMembers = 10
	require.NoError(t, mgr.Tick(ctx))

	// 分裂完成断言
	sh, err := bm.LoadFresh(ctx, tbl.Name, "idx_city", slot)
	require.NoError(t, err)
	e := sh.Eq["hot"]
	require.NotNil(t, e, "热值应有分裂条目")
	require.Nil(t, e.Split, "分裂应已完成（无中间态残留）")
	require.Equal(t, 2, len(e.Buckets), "1→2 子桶")

	// 数据完整（bm 感知读路径）
	ex := exec.New(cli, reg)
	ex.SetBucketMap(bm)
	require.Equal(t, 60, len(drainEq(t, ex, tbl, "hot")))
}

// TestL4Replicas L4 副本生命周期（docs/08 §8.4）：激活 → 副本服务 → 回收回退。
func TestL4Replicas(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	bm := bucketmap.New(cli, reg)
	g := txguard.New(cli, reg, bm)
	ctx := context.Background()
	tbl := splitTable()

	slot := uint16(654)
	pks := sameSlotPKs(tbl.Name, slot, 30, rand.New(rand.NewSource(99)))
	for _, pk := range pks {
		_, err := g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: pk, Fields: map[string]string{"city": "hot"}})
		require.NoError(t, err)
	}
	srcKey := keycodec.EqBucketKey(tbl.Name, "idx_city", "hot", slot, 0)

	l4 := NewL4(cli, reg)
	require.NoError(t, l4.Activate(ctx, tbl.Name, "idx_city", "hot", slot, srcKey, 3))

	// 读路径解析到副本（异 slot）
	rep, ok := l4.ReplicaFor(ctx, tbl.Name, "idx_city", "hot", srcKey, func(n int) int { return 0 })
	require.True(t, ok)
	require.NotEqual(t, keycodec.Slot(srcKey), keycodec.Slot(rep), "副本必须异 slot")

	// 副本内容与源桶一致
	m := 0
	res, err := cli.Do(ctx, "ZCARD", rep)
	require.NoError(t, err)
	m, _ = strconv.Atoi(toStr(res))
	require.Equal(t, 30, m, "副本含全部成员")

	// 回收后回退源桶
	require.NoError(t, l4.Deactivate(ctx, tbl.Name, "idx_city", "hot", slot, srcKey))
	_, ok = l4.ReplicaFor(ctx, tbl.Name, "idx_city", "hot", srcKey, func(n int) int { return 0 })
	require.False(t, ok)
	res, err = cli.Do(ctx, "EXISTS", rep)
	require.NoError(t, err)
	require.Equal(t, "0", toStr(res), "回收后副本被 UNLINK")
}
