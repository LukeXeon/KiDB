// Package indexer 消费异步索引日志（docs/05 §5.2）：
// LRANGE 批读 + LTRIM 截断，按行 slot 落桶（与同步索引同布局，查询路径零分叉）。
// 最终一致（秒级）：新行短暂查不到、旧行残留由回表校验消除——不会出错行。
package indexer

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"kidb"
	"kidb/keycodec"
	"kidb/meta"
	"kidb/rowcodec"
)

// Indexer 消费一个 (表, 索引, slot) 的日志。
type Indexer struct {
	cli   kidb.Store
	batch int
}

// New 构造。
func New(cli kidb.Store) *Indexer { return &Indexer{cli: cli, batch: 500} }

// ConsumeLog 消费一批日志条目（LTRIM 截断已消费前缀），返回消费条数。
// 消费语义：条目 pk\x1f旧值\x1f新值\x1fver；旧值非空 → ZREM 旧桶，新值非空 → ZADD 新桶。
// 乱序安全：同 pk 连续变更按日志序重放即可收敛（ZSet 幂等 + 回表校验兜底）。
func (i *Indexer) ConsumeLog(ctx context.Context, t *meta.TableDef, idx *meta.IndexDef, slot uint16) (int, error) {
	logkey := keycodec.AsyncLogKey(t.Name, idx.ID, slot)
	res, err := i.cli.Do(ctx, "LRANGE", logkey, 0, i.batch-1)
	if err != nil {
		return 0, err
	}
	entries := asStrings(res)
	if len(entries) == 0 {
		return 0, nil
	}

	var cmds []kidb.Cmd
	col := idx.Columns[0]
	colDef, colOK := t.Column(col)
	for _, e := range entries {
		parts := strings.SplitN(e, "\x1f", 4)
		if len(parts) != 4 {
			continue // 畸形条目跳过（对账兜底，docs/12 §12.8）
		}
		pk, oldV, newV := parts[0], parts[1], parts[2]
		rowSlot := keycodec.Slot(keycodec.RowKey(t.Name, pk))
		switch idx.Kind {
		case meta.IndexRange:
			if !colOK {
				break
			}
			bk := keycodec.RangeBucketKey(t.Name, idx.ID, rowSlot, 0)
			if oldV != "" {
				cmds = append(cmds, kidb.Cmd{Name: "ZREM", Args: []any{bk, pk}})
			}
			if newV != "" {
				score, err := rowcodec.ScoreOf(colDef.Type, newV)
				if err != nil {
					continue
				}
				cmds = append(cmds, kidb.Cmd{Name: "ZADD", Args: []any{bk, fmt.Sprint(score), pk}})
			}
		default: // IndexEq
			if oldV != "" {
				cmds = append(cmds, kidb.Cmd{Name: "ZREM", Args: []any{keycodec.EqBucketKey(t.Name, idx.ID, oldV, rowSlot, 0), pk}})
			}
			if newV != "" {
				cmds = append(cmds, kidb.Cmd{Name: "ZADD", Args: []any{keycodec.EqBucketKey(t.Name, idx.ID, newV, rowSlot, 0), 0, pk}})
			}
		}
		// 字典序副本随行（docs/03 §3.1）
		if idx.PrefixCopy {
			lk := keycodec.LexBucketKey(t.Name, idx.ID, rowSlot, 0)
			if oldV != "" {
				cmds = append(cmds, kidb.Cmd{Name: "ZREM", Args: []any{lk, oldV + "\x00" + pk}})
			}
			if newV != "" {
				cmds = append(cmds, kidb.Cmd{Name: "ZADD", Args: []any{lk, 0, newV + "\x00" + pk}})
			}
		}
	}
	if len(cmds) > 0 {
		if _, err := i.cli.Pipeline(ctx, cmds); err != nil {
			return 0, err
		}
	}

	// LTRIM 截断已消费前缀（批操作有界；失败留下重复条目由 ZSet 幂等吸收）
	if _, err := i.cli.Do(ctx, "LTRIM", logkey, len(entries), -1); err != nil {
		return 0, fmt.Errorf("indexer: LTRIM %s: %w", logkey, err)
	}
	return len(entries), nil
}

// LogBacklog 返回日志积压量（async_index_log_backlog 指标挂点）。
func (i *Indexer) LogBacklog(ctx context.Context, t *meta.TableDef, idx *meta.IndexDef, slot uint16) (int64, error) {
	res, err := i.cli.Do(ctx, "LLEN", keycodec.AsyncLogKey(t.Name, idx.ID, slot))
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(fmt.Sprint(res), 10, 64)
}

func asStrings(res any) []string {
	switch v := res.(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, e := range v {
			out = append(out, fmt.Sprint(e))
		}
		return out
	}
	return nil
}
