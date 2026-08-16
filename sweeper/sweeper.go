// Package sweeper 是分布式过期清扫（docs/07 §7.3，v7.0 集中登记册形态）：
// 到期发现（exp 集中册 ZRANGEBYSCORE）→ 回执取撤销信息 → 行 slot 分组
// sweep_batch.lua 活性复查+删回执 → 客户端段（异 slot）：桶/登记册/预约清理。
// 版本戳精确 member 使"复查后复活"交错安全（新 member 不受旧撤销影响）。
// 正确性不依赖 Sweeper 在线：全挂只会变慢，不会出错行（docs/01 §1.7）。
package sweeper

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"kidb/keycodec"
	"kidb/kv"
	"kidb/meta"
	"kidb/metrics"
	"kidb/script"
	"kidb/tuning"
	"kidb/utils"
)

// Sweeper 执行清扫。
type Sweeper struct {
	m          *metrics.Metrics // 指标（nil = no-op）
	cli        kv.Client
	reg        *script.Registry
	batch      int              // 每 tick 每册到期批大小（docs/10 sweeper_batch）
	maxBatches int              // 每 tick 每册批数上限（sweeper_max_batches_per_tick）
	clock      func() time.Time // nil = 服务端 TIME/本地回退
}

// New 构造（参数即 docs/10 §10.2 变量默认值）。
func New(cli kv.Client, reg *script.Registry) *Sweeper {
	return &Sweeper{cli: cli, reg: reg, batch: tuning.Get().Sweeper.Batch, maxBatches: tuning.Get().Sweeper.MaxBatchesPerTick}
}

// SetClock 注入时钟（测试用：与写入侧共享可推进的钟）。
func (s *Sweeper) SetClock(c func() time.Time) { s.clock = c }

// SetLimits 覆盖批参数（配置热更新挂点）。
func (s *Sweeper) SetLimits(batch, maxBatches int) { s.batch, s.maxBatches = batch, maxBatches }

// SweepShard 清扫指定表指定登记册分片一轮，返回清扫行数。供生产循环与测试直接驱动。
func (s *Sweeper) SweepShard(ctx context.Context, t *meta.TableDef, shard int) (int, error) {
	total := 0
	for b := 0; b < s.maxBatches; b++ {
		n, err := s.sweepBatch(ctx, t, shard)
		if err != nil {
			return total, err
		}
		total += n
		if n < s.batch { // 不足一批 = 该分片已清完
			break
		}
	}
	return total, nil
}

// sweepBatch 清扫一批到期行。
func (s *Sweeper) sweepBatch(ctx context.Context, t *meta.TableDef, shard int) (int, error) {
	now, err := s.nowUnix(ctx)
	if err != nil {
		return 0, err
	}
	expKey := keycodec.ExpKeyN(t.Name, shard, t.EffectiveExpShards())

	// 1. 到期 pk 批（有界）
	res, err := s.cli.Do(ctx, "ZRANGEBYSCORE", expKey, "-inf", "("+strconv.FormatInt(now, 10), "LIMIT", 0, s.batch)
	if err != nil {
		return 0, err
	}
	pks := utils.Strings(res)
	if len(pks) == 0 {
		return 0, nil
	}
	return s.sweepPks(ctx, t, pks)
}

// SweepPksForced 强制清扫指定 pk 集（跳过活性复查——DROP 清理车道专用：
// 表已删除，行/回执/桶/登记册全部清掉；偏斜窗口死行一并覆盖，docs/06 §6.3）。
// 生产清扫路径不走这里（活性复查是复活拦截的关键不变式）。
func (s *Sweeper) SweepPksForced(ctx context.Context, t *meta.TableDef, shard int, pks []string) (int, error) {
	if len(pks) == 0 {
		return 0, nil
	}
	return s.sweepPks(ctx, t, pks)
}

// sweepPks 清扫一批 pk（回执驱动：行 slot 复查删回执 → 客户端段清桶/登记册/预约）。
func (s *Sweeper) sweepPks(ctx context.Context, t *meta.TableDef, pks []string) (int, error) {
	// 2. 取回执（行 slot，批）
	cmds := make([]kv.Cmd, 0, len(pks))
	for _, pk := range pks {
		cmds = append(cmds, kv.Cmd{Name: "HGETALL", Args: []any{keycodec.ReceiptKey(t.Name, pk)}})
	}
	rcpts, err := s.cli.Pipeline(ctx, cmds)
	if err != nil {
		return 0, err
	}

	// 3. 按行 slot 分组执行 sweep_batch.lua（活性复查 + 删回执，同 slot 原子）
	type slotEntry struct{ pk string }
	groups := map[uint16][]int{} // slot → pk 下标
	for i, pk := range pks {
		slot := keycodec.Slot(keycodec.RowKey(t.Name, pk))
		groups[slot] = append(groups[slot], i)
	}
	cleaned := map[int]bool{}
	sb, ok := s.reg.Get("sweep_batch")
	if !ok {
		return 0, fmt.Errorf("sweeper: sweep_batch.lua not registered")
	}
	for _, idxs := range groups {
		keys := make([]string, 0, 2*len(idxs))
		for _, i := range idxs {
			keys = append(keys, keycodec.RowKey(t.Name, pks[i]))
		}
		for _, i := range idxs {
			keys = append(keys, keycodec.ReceiptKey(t.Name, pks[i]))
		}
		argv := []any{strconv.Itoa(len(idxs))}
		for j, i := range idxs {
			argv = append(argv, pks[i], strconv.Itoa(j+1), strconv.Itoa(j+1))
		}
		out, err := s.cli.Eval(ctx, sb, keys, argv...)
		if err != nil {
			return 0, err
		}
		for _, pk := range utils.Strings(out) {
			for _, i := range idxs {
				if pks[i] == pk {
					cleaned[i] = true
					break
				}
			}
		}
	}
	if len(cleaned) == 0 {
		return 0, nil
	}

	// 4. 客户端段（异 slot）：按回执 ZREM 桶（版本戳精确 member）+ ZREM 登记册 +
	//    释放唯一预约（占有者比对——只删仍属于死行的预约，防误删窗口内同值新写入的活预约）
	var zcmds []kv.Cmd
	type resvT struct{ rkey, ownerRow string }
	var reservations []resvT
	expKey := keycodec.ExpKeyN(t.Name, 0, t.EffectiveExpShards()) // 占位（逐 pk 取分片）
	_ = expKey
	for i, pk := range pks {
		if !cleaned[i] {
			continue
		}
		fields, _ := utils.StringMap(rcpts[i])
		for f, v := range fields {
			if strings.HasPrefix(f, "idx:") {
				parts := strings.SplitN(v, "\x1f", 2)
				if len(parts) == 2 {
					zcmds = append(zcmds, kv.Cmd{Name: "ZREM", Args: []any{parts[0], parts[1]}})
				}
			} else if strings.HasPrefix(f, "__uniq:") {
				reservations = append(reservations, resvT{v, keycodec.RowKey(t.Name, pk)})
			}
		}
		shards := t.EffectiveExpShards()
		zcmds = append(zcmds, kv.Cmd{Name: "ZREM", Args: []any{
			keycodec.ExpKeyN(t.Name, keycodec.ExpShardFor(pk, shards), shards), pk}})
	}
	if len(zcmds) > 0 {
		if _, err := s.cli.Pipeline(ctx, zcmds); err != nil {
			return 0, err
		}
	}
	for _, rv := range reservations {
		cur, err := s.cli.Do(ctx, "GET", rv.rkey)
		if err != nil || cur == nil {
			continue
		}
		if strings.SplitN(fmt.Sprint(cur), "|", 2)[0] != rv.ownerRow {
			continue
		}
		_, _ = s.cli.Do(ctx, "DEL", rv.rkey)
	}

	n := len(cleaned)
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
			if ss := utils.Strings(res); len(ss) > 0 {
				if n, err := strconv.ParseInt(ss[0], 10, 64); err == nil {
					return n, nil
				}
			}
		}
	}
	return time.Now().Unix(), nil
}
