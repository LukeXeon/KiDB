package controller

import (
	"context"
	"math/rand"
	"testing"

	"pgregory.net/rapid"

	"kidb/bucketmap"
	"kidb/exec"
	"kidb/internal/redistest"
	"kidb/txguard"
)

// TestSplitMergePBT 是 docs/12 §12.3 P1 的 rapid 形态：
// 随机 {写入、删除、触发分裂、触发合并} × 任意交错；
// 不变式：任意时刻等值查询结果 == 模型；状态机永不进非法状态（协议内异常即失败）。
func TestSplitMergePBT(t *testing.T) {
	if testing.Short() {
		t.Skip("PBT 长测（-short 跳过；CI 全量跑，docs/12 §12.9）")
	}
	slot := uint16(2048)
	values := []string{"red", "green", "blue"}

	cli, reg, m := redistest.New(t)
	ctx := context.Background()
	tbl := splitTable()
	pks := sameSlotPKs(tbl.Name, slot, 400, rand.New(rand.NewSource(31)))

	rapid.Check(t, func(rt *rapid.T) {
		// 每个 rapid 用例重置存储（组件轻量重建，避免状态跨用例泄漏）
		m.FlushAll()
		bm := bucketmap.New(cli, reg)
		g := txguard.New(cli, reg, bm)
		sp := NewSplitter(cli, reg, bm)
		e := exec.New(cli, reg)
		e.SetBucketMap(bm)

		model := map[string]map[string]bool{} // value → pks
		for _, v := range values {
			model[v] = map[string]bool{}
		}
		ops := 0
		rt.Repeat(map[string]func(*rapid.T){
			"write": func(rt *rapid.T) {
				pk := rapid.SampledFrom(pks).Draw(rt, "pk")
				val := rapid.SampledFrom(values).Draw(rt, "val")
				for a := 0; a < 5; a++ {
					if _, err := g.WriteRow(ctx, txguard.WriteReq{Table: tbl, PK: pk, Fields: map[string]string{"city": val}}); err == nil {
						model[val][pk] = true
						for _, v := range values {
							if v != val {
								delete(model[v], pk)
							}
						}
						break
					}
				}
				ops++
			},
			"delete": func(rt *rapid.T) {
				pk := rapid.SampledFrom(pks).Draw(rt, "pkdel")
				if _, err := g.DeleteRow(ctx, tbl, pk); err == nil {
					for _, v := range values {
						delete(model[v], pk)
					}
				}
				ops++
			},
			"split": func(rt *rapid.T) {
				val := rapid.SampledFrom(values).Draw(rt, "valsplit")
				if err := sp.SplitEq(ctx, tbl.Name, "idx_city", val, slot); err != nil {
					rt.Fatalf("split 协议错误: %v", err)
				}
				ops++
			},
			"merge": func(rt *rapid.T) {
				val := rapid.SampledFrom(values).Draw(rt, "valmerge")
				if err := sp.MergeEq(ctx, tbl.Name, "idx_city", val, slot); err != nil {
					rt.Fatalf("merge 协议错误: %v", err)
				}
				ops++
			},
			"check": func(rt *rapid.T) {
				val := rapid.SampledFrom(values).Draw(rt, "valchk")
				got := drainEq(t, e, tbl, val)
				if len(got) != len(model[val]) {
					rt.Fatalf("不变式破坏: %s 查询 %d 行, 模型 %d 行", val, len(got), len(model[val]))
				}
				for pk := range got {
					if !model[val][pk] {
						rt.Fatalf("不变式破坏: 查询出行 %s 不在模型 %s", pk, val)
					}
				}
			},
		})
		if ops < 5 {
			rt.Skip("操作数不足")
		}
	})
}
