package txguard

import (
	"context"

	"github.com/cespare/xxhash/v2"

	"kidb"
	"kidb/keycodec"
)

// hll.go：索引基数 HLL 的采样写入（docs/04 §4.6：结果必须精确，统计可以近似）。
//
// 形态：写入成功后按**值**确定性采样（xxhash64(idx|encVal) % 64 == 0），命中时
// `PFADD hll:{table}:{idx} 编码值`（单 pipeline，不进写入 Lua——HLL 是决策统计
// 而非正确性结构，无需原子性；Lua 保持瘦小）。
//
// 为什么按值采样而不是按 pk：按 pk 采样时高频值几乎必被采中（每值多次出现任一
// 命中即入 HLL），PFCOUNT×64 回补会把低基数列高估 ~64 倍；按值采样让"每个
// distinct 值以 1/64 概率整体入样"，PFCOUNT×64 是基数 D 的无偏估计（频率无关）。
//
// 已知漂移（诚实声明）：UPDATE 新值累积、DELETE 不回收——基数估值向上漂移，
// 与 HLL 自身误差同属"统计可近似"纪律。

// hllSampleRate 采样率分母（1/64）。
const hllSampleRate = 64

// HLLSampledValue 按值确定性采样（同 idx+value 恒同结果——重放/回填/增量同规则，
// 复制安全）。JobRunner 回填共用。
func HLLSampledValue(idxID, encVal string) bool {
	return xxhash.Sum64String(idxID+"\x00"+encVal)%hllSampleRate == 0
}

// HLLCompensation 采样回补倍数（读侧 PFCOUNT 乘此值）。
const HLLCompensation = hllSampleRate

// hllSample 写入成功后的采样 PFADD（尽力而为，失败仅损统计精度）。
func (g *Guard) hllSample(ctx context.Context, req WriteReq) {
	t := req.Table
	var cmds []kidb.Cmd
	for i := range t.Indexes {
		idx := &t.Indexes[i]
		if len(idx.Columns) != 1 {
			continue
		}
		v, ok := req.Fields[idx.Columns[0]]
		if !ok || v == "" {
			continue // NULL/缺失不入统计
		}
		if !HLLSampledValue(idx.ID, v) {
			continue
		}
		cmds = append(cmds, kidb.Cmd{Name: "PFADD", Args: []any{keycodec.HLLKey(t.Name, idx.ID), v}})
	}
	if len(cmds) == 0 {
		return
	}
	_, _ = g.cli.Pipeline(ctx, cmds) // 统计路径：失败不反传（近似纪律）
}
