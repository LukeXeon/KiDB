// Package sweeper 是分布式过期清扫（docs/07 §7.3）：
// 到期发现（exp 登记册 ZRANGEBYSCORE）→ 回执取撤销信息 → 唯一预约释放（异 slot）
// → sweep_batch.lua 单 slot 原子清扫（含复活复查）。
// 正确性不依赖 Sweeper 在线：全挂只会变慢，不会出错行（docs/01 §1.7）。
package sweeper

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"kidb"
	"kidb/ds"
	"kidb/keycodec"
	"kidb/meta"
	"kidb/metrics"
	"kidb/script"
	"kidb/tuning"
)

// Sweeper 执行清扫。
type Sweeper struct {
	m          *metrics.Metrics // 指标（nil = no-op）
	cli        kidb.KvClient
	reg        *script.Registry
	batch      int              // 每 tick 每 slot 到期批大小（docs/10 sweeper_batch）
	maxBatches int              // 每 tick 每 slot 批数上限（sweeper_max_batches_per_tick）
	clock      func() time.Time // nil = 服务端 TIME/本地回退
}

// New 构造（参数即 docs/10 §10.2 变量默认值）。
func New(cli kidb.KvClient, reg *script.Registry) *Sweeper {
	return &Sweeper{cli: cli, reg: reg, batch: tuning.Get().Sweeper.Batch, maxBatches: tuning.Get().Sweeper.MaxBatchesPerTick}
}

// SetClock 注入时钟（测试用：与写入侧共享可推进的钟）。
func (s *Sweeper) SetClock(c func() time.Time) { s.clock = c }

// SetLimits 覆盖批参数（配置热更新挂点）。
func (s *Sweeper) SetLimits(batch, maxBatches int) { s.batch, s.maxBatches = batch, maxBatches }

// SweepSlot 清扫指定表指定 slot 一轮（含分片登记册），返回清扫行数。
// 供生产循环与测试直接驱动。
func (s *Sweeper) SweepSlot(ctx context.Context, t *meta.TableDef, slot uint16) (int, error) {
	total := 0
	for shard := 0; shard < t.EffectiveExpShards(); shard++ {
		for b := 0; b < s.maxBatches; b++ {
			n, err := s.sweepBatch(ctx, t, slot, shard)
			if err != nil {
				return total, err
			}
			total += n
			if n < s.batch { // 不足一批 = 该分片已清完
				break
			}
		}
	}
	return total, nil
}

// sweepBatch 清扫一批到期行。
func (s *Sweeper) sweepBatch(ctx context.Context, t *meta.TableDef, slot uint16, shard int) (int, error) {
	now, err := s.nowUnix(ctx)
	if err != nil {
		return 0, err
	}
	expKey := keycodec.ExpKeyN(t.Name, slot, shard, t.EffectiveExpShards())

	// 1. 到期 pk 批（有界）
	res, err := s.cli.Do(ctx, "ZRANGEBYSCORE", expKey, "-inf", "("+strconv.FormatInt(now, 10), "LIMIT", 0, s.batch)
	if err != nil {
		return 0, err
	}
	pks := ds.Strings(res)
	if len(pks) == 0 {
		return 0, nil
	}

	// 2. 取回执（同 slot pipeline）
	cmds := make([]kidb.Cmd, 0, len(pks))
	for _, pk := range pks {
		cmds = append(cmds, kidb.Cmd{Name: "HGETALL", Args: []any{keycodec.ReceiptKey(t.Name, pk)}})
	}
	rcpts, err := s.cli.Pipeline(ctx, cmds)
	if err != nil {
		return 0, err
	}

	// 3. 组装 sweep_batch 参数 + 收集唯一预约（异 slot，Lua 外释放，docs/07 §7.3）
	var extraKeys []string // KEYS[3..] 段
	keyIdx := map[string]int{}
	ref := func(k string) int {
		if i, ok := keyIdx[k]; ok {
			return i
		}
		extraKeys = append(extraKeys, k)
		keyIdx[k] = len(extraKeys)
		return len(extraKeys)
	}
	var reservations []string

	argv := []any{strconv.FormatInt(now, 10), strconv.Itoa(len(pks))}
	for i, pk := range pks {
		fields, _ := ds.StringMap(rcpts[i])
		var buckets [][2]string
		for f, v := range fields {
			if strings.HasPrefix(f, "idx:") {
				parts := strings.SplitN(v, "\x1f", 2)
				if len(parts) == 2 {
					buckets = append(buckets, [2]string{parts[0], parts[1]})
				}
			} else if strings.HasPrefix(f, "__uniq:") {
				reservations = append(reservations, v)
			}
		}
		argv = append(argv, pk, strconv.Itoa(ref(keycodec.ReceiptKey(t.Name, pk))), strconv.Itoa(len(buckets)))
		for _, b := range buckets {
			argv = append(argv, strconv.Itoa(ref(b[0])), b[1])
		}
	}

	keys := append([]string{expKey, keycodec.CntKey(t.Name, slot)}, extraKeys...)
	sb, ok := s.reg.Get("sweep_batch")
	if !ok {
		return 0, fmt.Errorf("sweeper: sweep_batch.lua not registered")
	}
	out, err := s.cli.Eval(ctx, sb, keys, argv...)
	if err != nil {
		return 0, err
	}

	// 4. 释放唯一预约（异 slot DEL；占有者行已死，releaseUnique 的占有者比对在 Lua 提交后安全）
	for _, rkey := range reservations {
		_, _ = s.cli.Do(ctx, "DEL", rkey)
	}

	n, _ := strconv.Atoi(fmt.Sprint(out))
	if s.m != nil && n > 0 {
		s.m.SweptTotal.Add(float64(n)) // swept_total
	}
	return n, nil
}

// nowUnix 时钟：注入时钟优先（测试）；否则服务端 TIME（docs/09 §9.4 可选能力，
// 时钟偏移校准），再回退本地时钟。注意 miniredis 的 TIME 不随 FastForward 走
// （FastForward 只推进 TTL 时钟），故 TTL 测试必须注入共享钟。
func (s *Sweeper) nowUnix(ctx context.Context) (int64, error) {
	if s.clock != nil {
		return s.clock().Unix(), nil
	}
	if s.cli.Capabilities().ServerTime {
		res, err := s.cli.Do(ctx, "TIME")
		if err == nil {
			if ss := ds.Strings(res); len(ss) > 0 {
				if n, err := strconv.ParseInt(ss[0], 10, 64); err == nil {
					return n, nil
				}
			}
		}
	}
	return time.Now().Unix(), nil
}
