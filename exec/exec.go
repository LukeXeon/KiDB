// Package exec 是查询执行器（docs/04）：把 planner 翻译的物理计划
// 执行为全链路流式 RowStream——桶游标分页 → pk 批 → pipeline 回表 →
// 谓词校验 → 解码产出。一切命令有界（页 512），任何路径不物化全量。
//
// v1 边界：桶一律 ACTIVE 单桶形态（分裂状态机随 controller 落地扩展）；
// 谓词下推 Lua（docs/04 §4.2）与 L1~L4 热 key 防御随后续批次接入。
package exec

import (
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"kidb"
	"kidb/keycodec"
	"kidb/meta"
	"kidb/rowcodec"
)

// 常量默认值（docs/10 §10.2 变量表；config 包落地后改读全局变量）。
const (
	defaultBatch         = 512 // 分页/回表批大小
	defaultSlotsPerRound = 64  // 每轮 scatter 的 slot 组宽
)

// Kind 是物理计划类型（docs/04 §4.1 翻译表）。
type Kind int

const (
	FullScan    Kind = iota + 1 // exp 登记册遍历（docs/07 §7.4）
	PointGet                    // 主键点查：HGETALL 直取
	EqLookup                    // 等值索引：值 × slot 桶 ZRANGE 分页
	RangeLookup                 // 范围索引：slot 桶 ZRANGEBYSCORE 分页
)

// Predicate 是回表校验谓词（docs/04 §4.3：所有索引路径的必须环节）。
// 字段缺失按 NULL 处理——NULL 不满足任何比较（MySQL 三值逻辑的我们需要的子集）。
type Predicate struct {
	Column string       // 谓词列
	Eq     []string     // 等值集合（编码后）；nil = 无等值约束
	Ranges []RangeBound // 数值范围集合（OR 语义）；nil = 无范围约束
	Str    []StrRange   // 字符串范围集合（字典序，OR 语义）
}

// RangeBound 是一段 score 区间 [lo, hi]（开闭由 Open 位决定；
// ±∞ 用 math.Inf 表达）。
type RangeBound struct {
	Lo, Hi         float64
	LoOpen, HiOpen bool
}

// StrRange 是一段字符串区间（字典序）。
type StrRange struct {
	Lo, Hi         string
	LoInf, HiInf   bool // 无界
	LoOpen, HiOpen bool
}

// Match 对存储形态行做校验。
func (p *Predicate) Match(raw map[string]string) bool {
	if p == nil || p.Column == "" {
		return true
	}
	v, ok := raw[p.Column]
	if !ok {
		return false // NULL
	}
	if p.Eq != nil {
		for _, e := range p.Eq {
			if v == e {
				return true
			}
		}
		return false
	}
	if p.Ranges != nil {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return false
		}
		for _, r := range p.Ranges {
			if r.contains(f) {
				return true
			}
		}
		return false
	}
	if p.Str != nil {
		for _, r := range p.Str {
			if r.contains(v) {
				return true
			}
		}
		return false
	}
	return true
}

func (r RangeBound) contains(f float64) bool {
	if r.LoOpen {
		if f <= r.Lo {
			return false
		}
	} else if f < r.Lo {
		return false
	}
	if r.HiOpen {
		if f >= r.Hi {
			return false
		}
	} else if f > r.Hi {
		return false
	}
	return true
}

func (r StrRange) contains(v string) bool {
	if !r.LoInf {
		if r.LoOpen {
			if v <= r.Lo {
				return false
			}
		} else if v < r.Lo {
			return false
		}
	}
	if !r.HiInf {
		if r.HiOpen {
			if v >= r.Hi {
				return false
			}
		} else if v > r.Hi {
			return false
		}
	}
	return true
}

// Request 是一次查询执行的完整描述（对齐 docs/04 §4.3 planner.Request；
// v1 在 exec 内承载，planner 包落地后上移）。
type Request struct {
	Table *meta.TableDef
	Kind  Kind

	Pks    []string       // PointGet：编码后 pk 列表
	Index  *meta.IndexDef // EqLookup/RangeLookup：命中的索引
	Values []string       // EqLookup：编码后值列表
	Ranges []RangeBound   // RangeLookup：score 区间列表（OR，不重迭）

	Pred *Predicate // 回表校验（nil = 不校验）
}

// Executor 执行 Request，产出流式行。
type Executor struct {
	cli           kidb.Client
	batch         int
	slotsPerRound int
}

// New 构造执行器。
func New(cli kidb.Client) *Executor {
	return &Executor{cli: cli, batch: defaultBatch, slotsPerRound: defaultSlotsPerRound}
}

// RowStream 是流式行游标（io.EOF 结束）。调用方负责 Close。
type RowStream struct {
	req  *Request
	exec *Executor
	ctx  context.Context

	// 分页状态
	nextSlot int          // 下一个 slot 组起点
	rangeIdx int          // RangeLookup：当前区间下标
	pending  []bucketScan // 本组未完成桶游标
	rows     [][]any      // 已解码待产出行
	err      error
	closed   bool
	rowCount int64
}

type bucketScan struct {
	key    string
	cursor int // ZRANGE/ZRANGEBYSCORE LIMIT 的偏移游标
}

// Run 启动流式执行。
func (e *Executor) Run(ctx context.Context, req *Request) *RowStream {
	return &RowStream{req: req, exec: e, ctx: ctx}
}

// Next 产出下一行（已解码、已过校验）；结束返回 io.EOF。
func (s *RowStream) Next() ([]any, error) {
	for {
		if s.closed {
			return nil, io.EOF
		}
		if s.err != nil {
			return nil, s.err
		}
		if len(s.rows) > 0 {
			r := s.rows[0]
			s.rows = s.rows[1:]
			s.rowCount++
			return r, nil
		}
		if err := s.fill(); err != nil {
			if err == io.EOF {
				s.err = io.EOF
			}
			return nil, err
		}
	}
}

// Close 结束游标。
func (s *RowStream) Close() error {
	s.closed = true
	s.rows = nil
	return nil
}

// fill 拉取下一批行；无更多数据返回 io.EOF。
func (s *RowStream) fill() error {
	switch s.req.Kind {
	case PointGet:
		return s.fillPointGet()
	default:
		return s.fillScatter()
	}
}

// fillPointGet 主键点查：pipeline 批量 HGETALL，一次性消费（docs/04 §4.1）。
func (s *RowStream) fillPointGet() error {
	if s.nextSlot > 0 { // PointGet 只跑一轮
		return io.EOF
	}
	s.nextSlot = 1
	return s.fetchRows(s.req.Pks)
}

// fillScatter 桶/登记册散取：slot 组 → 桶游标分页 → 回表 → 校验。
func (s *RowStream) fillScatter() error {
	e := s.exec
	for {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		// 本组桶游标耗尽 → 推进到下一 slot 组；RangeLookup 逐区间推进
		if len(s.pending) == 0 {
			if s.nextSlot >= keycodec.NumSlots {
				s.rangeIdx++
				s.nextSlot = 0
				if s.req.Kind != RangeLookup || s.rangeIdx >= len(s.req.Ranges) {
					return io.EOF
				}
			}
			s.pending = s.buildGroup(s.nextSlot, min(s.nextSlot+e.slotsPerRound, keycodec.NumSlots))
			s.nextSlot += e.slotsPerRound
			if len(s.pending) == 0 {
				continue
			}
		}
		// 一轮分页：每个未完成桶拉一页
		cmds := make([]kidb.Cmd, 0, len(s.pending))
		for _, b := range s.pending {
			cmds = append(cmds, s.pageCmd(b))
		}
		results, err := e.cli.Pipeline(s.ctx, cmds)
		if err != nil {
			return fmt.Errorf("exec: scatter page: %w", err)
		}
		var pks []string
		rest := s.pending[:0]
		for i, b := range s.pending {
			members := asStrings(results[i])
			for _, m := range members {
				pks = append(pks, stripCovering(m))
			}
			if len(members) == e.batch { // 满页 = 可能还有
				b.cursor += len(members)
				rest = append(rest, b)
			}
		}
		s.pending = rest
		if len(pks) == 0 {
			continue
		}
		if err := s.fetchRows(pks); err != nil {
			return err
		}
		return nil
	}
}

// buildGroup 为 slot 组 [from,to) 生成桶游标集合。
func (s *RowStream) buildGroup(from, to int) []bucketScan {
	var out []bucketScan
	t := s.req.Table
	for slot := from; slot < to; slot++ {
		s16 := uint16(slot)
		switch s.req.Kind {
		case EqLookup:
			for _, v := range s.req.Values {
				out = append(out, bucketScan{key: keycodec.EqBucketKey(t.Name, s.req.Index.ID, v, s16, 0)})
			}
		case RangeLookup:
			out = append(out, bucketScan{key: keycodec.RangeBucketKey(t.Name, s.req.Index.ID, s16, 0)})
		case FullScan:
			for shard := 0; shard < t.EffectiveExpShards(); shard++ {
				out = append(out, bucketScan{key: keycodec.ExpKeyN(t.Name, s16, shard, t.EffectiveExpShards())})
			}
		}
	}
	return out
}

// pageCmd 生成一桶一页的命令（ZRANGE 家族，带 LIMIT——有界纪律 docs/04 §4.1）。
func (s *RowStream) pageCmd(b bucketScan) kidb.Cmd {
	batch := s.exec.batch
	if s.req.Kind == RangeLookup {
		r := s.req.Ranges[s.rangeIdx]
		return kidb.Cmd{Name: "ZRANGEBYSCORE", Args: []any{b.key, rangeBound(r.Lo, r.LoOpen), rangeBound(r.Hi, r.HiOpen), "LIMIT", b.cursor, batch}}
	}
	// EqLookup / FullScan：ZRANGE 偏移分页
	return kidb.Cmd{Name: "ZRANGE", Args: []any{b.key, b.cursor, b.cursor + batch - 1}}
}

// rangeBound 生成 ZRANGEBYSCORE 边界语法（开区间加 "(" 前缀；±∞ 为 -inf/+inf）。
func rangeBound(v float64, open bool) string {
	if math.IsInf(v, -1) {
		return "-inf"
	}
	if math.IsInf(v, 1) {
		return "+inf"
	}
	s := strconv.FormatFloat(v, 'g', -1, 64)
	if open {
		return "(" + s
	}
	return s
}

// fetchRows 批量回表（pipeline HGETALL，512 批）→ 空行跳过 → 谓词校验 → 解码入队。
// 这是"回表校验"纪律的落实点（docs/04 §4.3）：一切索引路径的输出都经此过滤。
func (s *RowStream) fetchRows(pks []string) error {
	t := s.req.Table
	for i := 0; i < len(pks); i += s.exec.batch {
		batch := pks[i:min(i+s.exec.batch, len(pks))]
		cmds := make([]kidb.Cmd, 0, len(batch))
		for _, pk := range batch {
			cmds = append(cmds, kidb.Cmd{Name: "HGETALL", Args: []any{keycodec.RowKey(t.Name, pk)}})
		}
		results, err := s.exec.cli.Pipeline(s.ctx, cmds)
		if err != nil {
			return fmt.Errorf("exec: fetch rows: %w", err)
		}
		for j, res := range results {
			raw := asStringMap(res)
			if len(raw) == 0 {
				continue // 空 Hash = 行过期/不存在 → 静默跳过
			}
			if !s.req.Pred.Match(raw) {
				continue // 谓词校验拦截（rowiter_rows_filtered_total 指标挂点）
			}
			s.rows = append(s.rows, rowcodec.DecodeRow(t, batch[j], raw))
		}
	}
	return nil
}

// stripCovering 剥掉桶 member 的覆盖列后缀（docs/03 §3.5："pk|col1|col2..."）。
func stripCovering(member string) string {
	if i := strings.IndexByte(member, '|'); i >= 0 {
		return member[:i]
	}
	return member
}

// asStrings 归一 ZRANGE 族返回为字符串切片。
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

// asStringMap 归一 HGETALL 返回为 map[string]string。
func asStringMap(res any) map[string]string {
	switch v := res.(type) {
	case map[string]string:
		return v
	case map[any]any:
		out := make(map[string]string, len(v))
		for k, val := range v {
			out[fmt.Sprint(k)] = fmt.Sprint(val)
		}
		return out
	case []any:
		out := make(map[string]string, len(v)/2)
		for i := 0; i+1 < len(v); i += 2 {
			out[fmt.Sprint(v[i])] = fmt.Sprint(v[i+1])
		}
		return out
	}
	return nil
}

// RowCount 精确行数（docs/04 §4.1：Σ ZCOUNT(exp, (now, +inf))，任意时刻精确）。
// 供 StatisticsTable 与后续 COUNT(*) 下推使用。
func (e *Executor) RowCount(ctx context.Context, t *meta.TableDef, nowUnixSec int64) (uint64, error) {
	var cmds []kidb.Cmd
	for slot := 0; slot < keycodec.NumSlots; slot++ {
		for shard := 0; shard < t.EffectiveExpShards(); shard++ {
			cmds = append(cmds, kidb.Cmd{
				Name: "ZCOUNT",
				Args: []any{keycodec.ExpKeyN(t.Name, uint16(slot), shard, t.EffectiveExpShards()), "(" + strconv.FormatInt(nowUnixSec, 10), "+inf"},
			})
		}
	}
	results, err := e.cli.Pipeline(ctx, cmds)
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, r := range results {
		n, _ := strconv.ParseUint(fmt.Sprint(r), 10, 64)
		total += n
	}
	return total, nil
}
