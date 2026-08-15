package exec

import (
	"context"
	"fmt"
	"strconv"

	"kidb/keycodec"
	"kidb/txguard"
)

// stats.go：索引基数统计的读侧（docs/04 §4.6）。

// IndexCardinality 索引基数估算（distinct 值个数）：PFCOUNT × 采样回补。
// 近似（HLL ~0.8% + 采样误差 + 更新/删除向上漂移），只用于决策依据
// （优化器选路/DDL 评审），绝不进查询结果（结果精确纪律，docs/01 §1.7）。
func (e *Executor) IndexCardinality(ctx context.Context, table, idxID string) (uint64, error) {
	res, err := e.readDo(ctx, "PFCOUNT", keycodec.HLLKey(table, idxID))
	if err != nil {
		return 0, fmt.Errorf("exec: pfcount %s: %w", idxID, err)
	}
	n, err := strconv.ParseUint(fmt.Sprint(res), 10, 64)
	if err != nil {
		return 0, nil // 空 HLL/异常值按 0 基数（近似统计不报错）
	}
	return n * txguard.HLLCompensation(), nil
}
