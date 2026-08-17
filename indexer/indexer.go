// Package indexer 消费异步索引日志（docs/05 §5.2）：
// LRANGE 批读 + LTRIM 截断，按行 slot 落桶（与同步索引同布局，查询路径零分叉）。
// 最终一致（秒级）：新行短暂查不到、旧行残留由回表校验消除——不会出错行。
package indexer

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"kidb/keycodec"
	"kidb/kv"
	"kidb/meta"
	"kidb/metrics"
	"kidb/rowcodec"
	"kidb/utils"
)

// Indexer 消费一个 (表, 索引, slot) 的日志。
type Indexer struct {
	cli   kv.Client
	batch int
	m     *metrics.Metrics // 指标（nil = no-op；index_dlq_total）
}

// New 构造。
func New(cli kv.Client) *Indexer { return &Indexer{cli: cli, batch: 500} }

// SetMetrics 接入指标（"注册即接线"纪律）。
func (i *Indexer) SetMetrics(m *metrics.Metrics) { i.m = m }

// ConsumeLog 消费一批日志条目（LTRIM 截断已消费前缀），返回消费条数。
// 消费语义：条目 pk\x1f旧值\x1f新值\x1fver；旧值非空 → ZREM 旧桶，新值非空 → ZADD 新桶。
// 乱序安全：同 pk 连续变更按日志序重放即可收敛（ZSet 幂等 + 回表校验兜底）。
func (i *Indexer) ConsumeLog(ctx context.Context, t *meta.TableDef, idx *meta.IndexDef, slot uint16) (int, error) {
	logkey := keycodec.AsyncLogKey(t.Name, idx.ID, slot)
	res, err := i.cli.Do(ctx, "LRANGE", logkey, 0, i.batch-1)
	if err != nil {
		return 0, err
	}
	entries := utils.Strings(res)
	if len(entries) == 0 {
		return 0, nil
	}

	var cmds []kv.Cmd
	var dlq []string // 死信（畸形/硬失败条目——v7.0 触发四③落点）
	col := idx.Columns[0]
	colDef, colOK := t.Column(col)
	for _, e := range entries {
		parts := strings.SplitN(e, "\x1f", 5)
		if len(parts) != 5 {
			dlq = append(dlq, e) // 畸形条目落死信（不再静默丢弃）
			continue
		}
		pk := parts[0]
		// 日志字段是 url.QueryEscape 可逆形态（txguard escLogField）——
		// 解回原始值再按 keycodec 规则建桶（直接复用转义串会双重转义错位）。
		oldV, err1 := url.QueryUnescape(parts[1])
		newV, err2 := url.QueryUnescape(parts[2])
		oldIV, err3 := strconv.ParseUint(parts[3], 10, 64)
		ver, err4 := strconv.ParseUint(parts[4], 10, 64)
		if err1 != nil || err2 != nil || err3 != nil || err4 != nil || ver == 0 {
			dlq = append(dlq, e)
			continue
		}
		// v7.0 版本戳：撤销用 oldIV（member 创建版本，行内 _iv 纪律——值最后一次
		// 写入时的版本，docs/05 §5.1）；重建用 ver（本次写入版本）。墓碑（newV 空）
		// 只撤不建。
		switch idx.Kind {
		case meta.IndexRange:
			if !colOK {
				break
			}
			bk := keycodec.RangeBucketKey(t.Name, idx.ID, 0)
			if oldV != "" {
				cmds = append(cmds, kv.Cmd{Name: "ZREM", Args: []any{bk, rowcodec.PlainMember(pk, oldIV)}})
			}
			if newV != "" {
				score, err := rowcodec.ScoreOf(colDef.Type, newV)
				if err != nil {
					continue
				}
				cmds = append(cmds, kv.Cmd{Name: "ZADD", Args: []any{bk, fmt.Sprint(score), rowcodec.PlainMember(pk, ver)}})
			}
		default: // IndexEq
			if oldV != "" {
				cmds = append(cmds, kv.Cmd{Name: "ZREM", Args: []any{keycodec.EqBucketKey(t.Name, idx.ID, oldV, 0), rowcodec.PlainMember(pk, oldIV)}})
			}
			if newV != "" {
				cmds = append(cmds, kv.Cmd{Name: "ZADD", Args: []any{keycodec.EqBucketKey(t.Name, idx.ID, newV, 0), 0, rowcodec.PlainMember(pk, ver)}})
			}
		}
		// 字典序副本随行（docs/03 §3.1）
		if idx.PrefixCopy {
			lk := keycodec.LexBucketKey(t.Name, idx.ID, 0)
			if oldV != "" {
				cmds = append(cmds, kv.Cmd{Name: "ZREM", Args: []any{lk, rowcodec.LexMember(oldV, pk, oldIV)}})
			}
			if newV != "" {
				cmds = append(cmds, kv.Cmd{Name: "ZADD", Args: []any{lk, 0, rowcodec.LexMember(newV, pk, ver)}})
			}
		}
	}
	if len(cmds) > 0 {
		if _, err := i.cli.Pipeline(ctx, cmds); err != nil {
			// 应用失败重试一次（ZADD/ZREM 幂等可重放）；仍失败 = 硬失败
			// （桶被 DROP/节点持续故障）→ 整批落死信，不死循环（v7.0 触发四③）
			if _, err2 := i.cli.Pipeline(ctx, cmds); err2 != nil {
				dlq = append(dlq, entries...)
			}
		}
	}
	// 死信落点：dlq:idx:{table}:{idx} + 指标（对账巡检扩一项，docs/12 §12.8）
	if len(dlq) > 0 {
		args := []any{keycodec.DLQKey(t.Name, idx.ID)}
		for _, e := range dlq {
			args = append(args, e)
		}
		_, _ = i.cli.Do(ctx, "RPUSH", args...)
		if i.m != nil {
			i.m.IndexDLQ.Add(float64(len(dlq))) // index_dlq_total
		}
	}

	// LTRIM 截断已消费前缀（批操作有界；失败留下重复条目由 ZSet 幂等吸收）
	if _, err := i.cli.Do(ctx, "LTRIM", logkey, len(entries), -1); err != nil {
		return 0, fmt.Errorf("indexer: LTRIM %s: %w", logkey, err)
	}
	return len(entries), nil
}
