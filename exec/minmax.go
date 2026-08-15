package exec

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"kidb"
	"kidb/keycodec"
	"kidb/meta"
	"kidb/rowcodec"
)

// minmax.go：MIN/MAX 的端点查询（docs/04 §4.5）：
// 多桶端点候选 → 全局归并 → 回表校验（过期/改值的脏 member 跳过）→ 首个有效即答案。
// 结果精确纪律：候选必须过回表校验（桶内可能有过期未清扫/改值残留成员）。

// MinMax 返回范围索引的端点值（isMin=true 取 MIN）。无有效成员返回 found=false。
func (e *Executor) MinMax(ctx context.Context, t *meta.TableDef, idx *meta.IndexDef, isMin bool) (score float64, pk string, found bool, err error) {
	col := idx.Columns[0]
	colDef, ok := t.Column(col)
	if !ok {
		return 0, "", false, kidb.ErrContractViolation
	}

	// 每 slot 桶的光标（端点向内推进）
	cursors := map[string]int{} // bucketKey → 已消费位置
	var pending []string        // 待查桶
	for slot := 0; slot < keycodec.NumSlots; slot++ {
		pending = append(pending, keycodec.RangeBucketKey(t.Name, idx.ID, uint16(slot), 0))
	}

	for len(pending) > 0 {
		// 一轮：每桶取当前端点候选
		var cmds []kidb.Cmd
		var keys []string
		for _, bk := range pending {
			c := cursors[bk]
			if isMin {
				cmds = append(cmds, kidb.Cmd{Name: "ZRANGE", Args: []any{bk, c, c, "WITHSCORES"}})
			} else {
				cmds = append(cmds, kidb.Cmd{Name: "ZREVRANGE", Args: []any{bk, c, c, "WITHSCORES"}})
			}
			keys = append(keys, bk)
		}
		results, perr := e.readPipeline(ctx, cmds)
		if perr != nil {
			return 0, "", false, perr
		}

		// 归并：取全局最优候选
		type cand struct {
			bk    string
			score float64
			pk    string
		}
		var best *cand
		var next []string
		for i, bk := range keys {
			arr := asStrings(results[i])
			if len(arr) < 2 {
				continue // 桶已穷尽
			}
			next = append(next, bk) // 还有成员
			f, cerr := strconv.ParseFloat(arr[1], 64)
			if cerr != nil {
				continue
			}
			c := cand{bk: bk, score: f, pk: stripCoveringOf(idx, arr[0])}
			if best == nil || (isMin && c.score < best.score) || (!isMin && c.score > best.score) {
				b2 := c
				best = &b2
			}
		}
		if best == nil {
			return 0, "", false, nil // 全空
		}

		// 回表校验最优候选
		res, herr := e.readDo(ctx, "HGETALL", keycodec.RowKey(t.Name, best.pk))
		if herr != nil {
			return 0, "", false, herr
		}
		raw := asStringMap(res)
		valid := false
		if len(raw) > 0 {
			if vs, ok := raw[col]; ok {
				if f, cerr := rowScoreOf(colDef.Type, vs); cerr == nil && f == best.score {
					valid = true
				}
			}
		}
		if valid {
			return best.score, best.pk, true, nil
		}
		// 脏候选：推进该桶光标重试（过期/改值残留顺路跳过）
		cursors[best.bk]++
		pending = next
		if math.IsNaN(best.score) {
			return 0, "", false, fmt.Errorf("exec: NaN score in %s", best.bk)
		}
	}
	return 0, "", false, nil
}

// stripCoveringOf member → pk（按索引定义感知覆盖列编码）。
func stripCoveringOf(idx *meta.IndexDef, member string) string {
	return rowcodec.MemberPK(member, len(idx.Covering) > 0)
}

func rowScoreOf(ct meta.ColumnType, encoded string) (float64, error) {
	return strconv.ParseFloat(encoded, 64)
}
