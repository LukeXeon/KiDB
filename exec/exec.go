// Package exec 是查询执行器（docs/04）：把 planner 翻译的物理计划
// 执行为全链路流式 RowStream——桶游标分页 → pk 批 → pipeline 回表 →
// 谓词校验 → 解码产出。一切命令有界（页 512），任何路径不物化全量。
//
// 分裂状态机经 bucketmap 接入（SetBucketMap）；谓词下推（SetPushdown 注册表）
// 与 L1/L4（SetNearCache/SetL4）均已接线。
package exec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"kidb"
	"kidb/bucketmap"
	"kidb/keycodec"
	"kidb/meta"
	"kidb/metrics"
	"kidb/rowcodec"
	"kidb/script"
	"kidb/tuning"
	"kidb/utils"
)

// 批大小/组宽等调优值统一收敛 internal/tuning（docs/01 §1.0：开发者参数单一调优面）。

// Kind 是物理计划类型（docs/04 §4.1 翻译表）。
type Kind int

const (
	FullScan     Kind = iota + 1 // exp 登记册遍历（docs/07 §7.4）
	PointGet                     // 主键点查：HGETALL 直取
	EqLookup                     // 等值索引：值 × slot 桶 ZRANGE 分页
	RangeLookup                  // 范围索引：slot 桶 ZRANGEBYSCORE 归并（topk.go）
	PrefixLookup                 // 前缀搜索：字典序副本 ZRANGEBYLEX 归并（prefix.go）
)

// Predicate 是回表校验谓词（docs/04 §4.3：所有索引路径的必须环节）。
// 字段缺失按 NULL 处理——NULL 不满足任何比较（MySQL 三值逻辑的我们需要的子集）。
type Predicate struct {
	Column string       // 谓词列
	Eq     []string     // 等值集合（编码后）；nil = 无等值约束
	Ranges []RangeBound // 数值范围集合（OR 语义）；nil = 无范围约束
	Str    []StrRange   // 字符串范围集合（字典序，OR 语义）
	// LikePrefix 常量前缀（LIKE 'abc%' 的前缀搜索路径，docs/04 §4.5）——
	// 回表以 HasPrefix 重判（member 桶内值天然满足，此行兜 member 编码边界）。
	LikePrefix string
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
	if p.LikePrefix != "" {
		return strings.HasPrefix(v, p.LikePrefix)
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
	Index  *meta.IndexDef // EqLookup/RangeLookup/PrefixLookup：命中的索引
	Values []string       // EqLookup：编码后值列表
	Ranges []RangeBound   // RangeLookup：score 区间列表（OR，不重迭）

	// LexLo/LexHi：PrefixLookup 的 ZRANGEBYLEX 界（原始界值，命令侧加 [ 括号）。
	LexLo, LexHi string

	Pred     *Predicate // 回表校验（nil = 不校验）
	Pushdown bool       // 谓词下推到服务端 Lua（docs/04 §4.2，白名单形态）

	// Desc：RangeLookup 的归并方向（gms lookup.IsReverse → ORDER BY ... DESC）。
	// RangeLookup 始终产出全局 score 有序流（topk.go 头注：gms 删 Sort 契约）。
	Desc bool

	// Projection 输出列（gms ProjectedTable 下推；nil = 全列，空非 nil = 零宽行）。
	// 回表只取 投影 ∪ 谓词列（HMGET 替代 HGETALL，docs/04 §4.3 投影下推）。
	Projection []string
	// Covering：索引覆盖 投影∪谓词 全部列（translate 判定，docs/03 §3.5）——
	// 跳过回表：member 解码覆盖列 + exp 登记册 ZSCORE 活性校验。
	Covering bool

	// SlotLo/SlotHi：FullScan 的 slot 区间限定（DDL 回填分批游标用；
	// 0 值 = 全量 [0, 16384)）。
	SlotLo, SlotHi int
}

// Executor 执行 Request，产出流式行。
type Executor struct {
	cli           kidb.KvClient
	reg           *script.Registry   // 谓词下推脚本（nil = 不下推）
	nc            L1Cache            // L1 近缓存（nil = 关闭，docs/08 §8.4）
	bm            *bucketmap.Store   // 桶路由（分裂状态）；nil = 永远默认单桶
	l4            L4Resolver         // L4 热桶副本（nil = 关闭）
	telemetry     TelemetrySink      // 遥测采样（nil = 关闭）
	m             *metrics.Metrics   // 指标（nil = no-op，docs/10 §10.3）
	clock         func() time.Time   // 覆盖读路径活性判定时钟（测试可注入，与写入侧共钟）
	replicaRead   atomic.Bool        // L3 副本读开关（SetReplicaRead；docs/08 §8.4）
	rowCache      RowCache           // 行级近缓存（nil = 关闭，默认关闭；hotkey_row_cache 变量驱动）
	sf            singleflight.Group // L2 请求合并（docs/08 §8.4：同指纹并发合并；随 L1 装配生效）
	fsMu          sync.Mutex
	fsSem         chan struct{} // 全扫并发信号量（query_fullscan_rate_limit，docs/07 §7.4 限流通道）
	batch         int
	slotsPerRound int
}

// SetMetrics 接入指标。
func (e *Executor) SetMetrics(m *metrics.Metrics) { e.m = m }

// Metrics 当前指标（网关慢查询日志等外侧记录点用；nil = no-op）。
func (e *Executor) Metrics() *metrics.Metrics { return e.m }

// SetClock 注入时钟（测试与写入侧共享可推进的钟——miniredis TIME 不随
// FastForward 走，TTL 语义断言须全链路同钟）。
func (e *Executor) SetClock(c func() time.Time) { e.clock = c }

// SetL4 接入 L4 热桶副本解析。
func (e *Executor) SetL4(r L4Resolver) { e.l4 = r }

// SetReplicaRead 开关 L3 副本读（docs/08 §8.4：能力存在且策略允许时，
// 只读散取/回表分流到副本——回表校验兜底副本滞后，最终一致窗口见文档）。
// 网关按 `replica_read` 变量 + 适配器能力位驱动本开关。
func (e *Executor) SetReplicaRead(on bool) { e.replicaRead.Store(on) }

// readPipeline 读路径批命令出口：L3 开启且适配器声明能力时分流到副本
// （exec 发出的 Pipeline 命令全部是只读族——分流安全；Lua/元数据不经此路）。
func (e *Executor) readPipeline(ctx context.Context, cmds []kidb.Cmd) ([]any, error) {
	if e.replicaRead.Load() && e.cli.Capabilities().ReplicaRead {
		return e.cli.PipelineReplica(ctx, cmds)
	}
	return e.cli.Pipeline(ctx, cmds)
}

// readDo 单命令读出口（同 readPipeline 分流语义）。
func (e *Executor) readDo(ctx context.Context, cmd string, args ...any) (any, error) {
	if e.replicaRead.Load() && e.cli.Capabilities().ReplicaRead {
		return e.cli.DoReplica(ctx, cmd, args...)
	}
	return e.cli.Do(ctx, cmd, args...)
}

// SetTelemetry 接入遥测采样。
func (e *Executor) SetTelemetry(t TelemetrySink) { e.telemetry = t }

// SetBucketMap 接入 BucketMap（分裂状态感知读路径，docs/08 §8.3）。
func (e *Executor) SetBucketMap(bm *bucketmap.Store) { e.bm = bm }

// L1Cache 是谓词指纹→pk 列表的近缓存接口（nearcache.ShardedCache[[]string] 满足）。
// 正确性纪律：缓存只存 pk 列表，回表 + 谓词校验照常执行——陈旧列表至多
// 漏掉 TTL 窗口内的新行（3s，文档化语义），绝不会出错行。
type L1Cache interface {
	Get(key string) ([]string, bool)
	Add(key string, val []string)
}

// RowCache 是行级近缓存接口（nearcache.RowCache 满足，docs/08 §8.4 覆盖边界）。
// 条目 TTL ≤ 行剩余 TTL——过期行绝不返回；更新陈旧窗口 ≤ 默认 TTL
// （最终一致文档化语义，默认关闭，hotkey_row_cache 变量开启）。
type RowCache interface {
	Get(key string) (map[string]string, bool)
	Add(key string, fields map[string]string, rowTTL time.Duration)
}

// SetRowCache 接入行级近缓存（nil = 关闭；默认关闭是有意取舍）。
func (e *Executor) SetRowCache(c RowCache) { e.rowCache = c }

// L4Resolver 是热桶副本解析接口（controller.L4Manager 满足，docs/08 §8.4）：
// 若值有 L4 副本，返回一个随机副本 key（异 slot）。
type L4Resolver interface {
	ReplicaFor(ctx context.Context, table, idxID, encVal, srcBucketKey string, randFn func(int) int) (string, bool)
}

// TelemetrySink 是遥测采样出口（telemetry.Recorder 满足，docs/08 §8.1）：
// 1/64 概率采样命中时上报桶 key。
type TelemetrySink interface {
	Sample(ctx context.Context, bucketKey string)
}

// New 构造执行器（reg 供 pushdown_filter 服务端下推，docs/04 §4.2）。
func New(cli kidb.KvClient, reg *script.Registry) *Executor {
	tn := tuning.Get()
	return &Executor{cli: cli, reg: reg, clock: time.Now, batch: tn.Exec.Batch, slotsPerRound: tn.Exec.SlotsPerRound,
		fsSem: make(chan struct{}, tn.Exec.FullscanConcurrency)}
}

// SetFullscanLimit 全扫并发上限热更（query_fullscan_rate_limit 变量驱动）。
// 换通道只影响新查询；在途全扫持有旧通道引用，释放各归其道。
func (e *Executor) SetFullscanLimit(n int) {
	if n <= 0 {
		n = 1
	}
	e.fsMu.Lock()
	defer e.fsMu.Unlock()
	if cap(e.fsSem) != n {
		e.fsSem = make(chan struct{}, n)
	}
}

// acquireFullscan 获取全扫槽位（ctx 取消贯穿，契约 R6）。
func (e *Executor) acquireFullscan(ctx context.Context) (chan struct{}, error) {
	e.fsMu.Lock()
	sem := e.fsSem
	e.fsMu.Unlock()
	select {
	case sem <- struct{}{}:
		return sem, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// SetNearCache 接入 L1（nearcache_ttl 变量热更新的挂点）。
func (e *Executor) SetNearCache(c L1Cache) { e.nc = c }

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

	// L1 近缓存（仅 EqLookup；全扫指纹不入缓存，docs/08 §8.4）
	ncFP      string   // 谓词指纹
	ncCached  []string // 缓存命中的 pk 列表（ncCachedOff 分页消费）
	ncOff     int
	ncCollect []string // 散取路径收集的 pk（完全排空后才写缓存）
	pkOnly    bool     // L2 收集模式：只收集 pk 不回表（collectEqPKs）

	// 分裂窗口双读去重（SPLITTING 父子桶同 member 各一份）；
	// RangeLookup 多区间时跨区间去重（重叠区间谓词的防御，gms 理论上已合并）
	seen map[string]struct{}

	// bm 路由判定（Run 期解析）：等值值是否热分裂 / 范围索引是否有分裂
	bmHotEq    bool
	bmHotRange bool
	bmHotLex   bool // 字典序副本分裂（Eq["l"] 伪条目，prefix.go）

	om *orderedMerger // RangeLookup 当前区间的归并器（topk.go）
	lm *lexMerger     // PrefixLookup 的归并器（prefix.go）

	fsSem chan struct{} // 全扫限流槽位（持有至 EOF/Close，docs/07 §7.4）

	startedAt time.Time // 首个 Next 时间（query_duration_seconds 挂点）
}

type bucketScan struct {
	key    string
	cursor int    // ZRANGE/ZRANGEBYSCORE LIMIT 的偏移游标
	val    string // EqLookup：该桶的等值编码值（覆盖索引读路径重建 raw 用）
}

// candItem 一个散取/归并候选：桶 member + 提取的 pk + 索引列编码值。
// val：等值桶 = 桶值；范围桶 = score 反编码（覆盖读路径重建索引列字段用）。
type candItem struct {
	member string
	pk     string
	val    string
}

// Run 启动流式执行。
func (e *Executor) Run(ctx context.Context, req *Request) *RowStream {
	s := &RowStream{req: req, exec: e, ctx: ctx}
	// L1：等值查询查指纹缓存（命中 → 跳过散取直接回表；陈旧由校验兜底）。
	// 覆盖索引请求不走 L1——member 自带覆盖列，缓存 pk 列表反而丢覆盖红利。
	if req.Kind == EqLookup && e.nc != nil && req.Index != nil && !req.Covering {
		s.ncFP = l1Fingerprint(req)
		if pks, ok := e.nc.Get(s.ncFP); ok {
			s.ncCached = pks
			if e.m != nil {
				e.m.NearcacheHits.Inc()
			}
		} else {
			if e.m != nil {
				e.m.NearcacheMiss.Inc()
			}
			// L2 请求合并（docs/08 §8.4）：同指纹并发查询共享一次散取的
			// pk 列表物化（leader 收集，followers 经 singleflight 共享；
			// 回表校验各调用方独立执行——共享语义与 L1 完全一致）。
			// leader 物化带 cap（l2MaxCollect）：超大值域各调用方退回独立流式。
			s.initRoutes(ctx)
			ch := e.sf.DoChan(s.ncFP, func() (any, error) {
				// WithoutCancel：leader 客户端断开不连坐 followers；
				// 收集有界（cap）保证孤儿工作可弃
				pks, err := e.collectEqPKs(context.WithoutCancel(ctx), req)
				if err != nil {
					return nil, err
				}
				e.nc.Add(s.ncFP, pks) // leader 填充 L1（docs/08 §8.4 L1/L2 接力）
				return pks, nil
			})
			select {
			case r := <-ch:
				if r.Err == nil {
					s.ncCached = r.Val.([]string)
				} else if !errors.Is(r.Err, errL2Overflow) {
					s.err = r.Err // leader 失败共享给同指纹 followers（重试语义一致）
				} // errL2Overflow：退回普通流式散取（s.ncCached 保持 nil）
			case <-ctx.Done():
				s.err = ctx.Err()
			}
			return s
		}
	}
	s.initRoutes(ctx)
	return s
}

// initRoutes bm 路由判定与去重集合初始化（Run 与 L2 收集共用）。
func (s *RowStream) initRoutes(ctx context.Context) {
	e := s.exec
	req := s.req
	// bm 路由判定（热值注册表一次解析；稀疏原则——无分裂零额外读，docs/03 §3.1）
	if e.bm != nil && req.Index != nil {
		switch req.Kind {
		case EqLookup:
			s.seen = map[string]struct{}{}
			if reg, err := e.bm.Registry(ctx, req.Table.Name, req.Index.ID); err == nil {
				for _, v := range req.Values {
					if reg[keycodec.EscapeValue(v)] {
						s.bmHotEq = true
						break
					}
				}
			}
		case RangeLookup:
			s.seen = map[string]struct{}{}
			if reg, err := e.bm.Registry(ctx, req.Table.Name, req.Index.ID); err == nil && reg["@range"] {
				s.bmHotRange = true
			}
		}
	}
	// RangeLookup 多区间：跨区间去重（与分裂双读共用 seen；单区间零开销）
	if req.Kind == RangeLookup && len(req.Ranges) > 1 && s.seen == nil {
		s.seen = map[string]struct{}{}
	}
	// PrefixLookup：字典序副本分裂感知（Eq["l"] 伪条目；当前控制器不触发，
	// 恒默认桶——读侧挂点先行，启用时双读去重由 seen 承载）
	if req.Kind == PrefixLookup && e.bm != nil && req.Index != nil {
		if reg, err := e.bm.Registry(ctx, req.Table.Name, req.Index.ID); err == nil && reg["l"] {
			s.bmHotLex = true
			if s.seen == nil {
				s.seen = map[string]struct{}{}
			}
		}
	}
}

// errL2Overflow L2 收集上限（等值值域成员数不可由桶上限约束——
// 50k 上限是"每值每 slot"，全集群同值成员可超百万；超出即放弃共享物化，
// 各调用方退回独立流式散取，有界性优先）。
var errL2Overflow = errors.New("exec: L2 collect overflow")

// collectEqPKs L2 leader 的全量散取收集（只取 pk 不回表；与 fillScatter
// 同一分页循环，pkOnly 模式）。
func (e *Executor) collectEqPKs(ctx context.Context, req *Request) ([]string, error) {
	s := &RowStream{req: req, exec: e, ctx: ctx, pkOnly: true}
	s.initRoutes(ctx)
	for {
		err := s.fillScatter()
		if err == io.EOF {
			return s.ncCollect, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// l1Fingerprint 谓词指纹（table|idx|values 排序后拼接）。
func l1Fingerprint(req *Request) string {
	vs := append([]string(nil), req.Values...)
	sort.Strings(vs)
	return req.Table.Name + "|" + req.Index.ID + "|" + strings.Join(vs, ",")
}

// Next 产出下一行（已解码、已过校验）；结束返回 io.EOF。
func (s *RowStream) Next() ([]any, error) {
	if s.startedAt.IsZero() {
		s.startedAt = time.Now()
	}
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
				// 完全排空才把散取结果写进 L1（部分消费/提前终止不入缓存）
				if s.ncFP != "" && s.ncCached == nil && s.exec.nc != nil {
					s.exec.nc.Add(s.ncFP, s.ncCollect)
				}
				s.releaseFullscan()
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
	s.releaseFullscan()
	if s.exec.m != nil && !s.startedAt.IsZero() {
		s.exec.m.QueryDuration.WithLabelValues(s.planName()).Observe(time.Since(s.startedAt).Seconds())
	}
	return nil
}

// releaseFullscan 释放全扫限流槽位（幂等）。
func (s *RowStream) releaseFullscan() {
	if s.fsSem != nil {
		<-s.fsSem
		s.fsSem = nil
	}
}

// planName 返回计划名（query_duration_seconds{plan} 标签）。
func (s *RowStream) planName() string {
	switch s.req.Kind {
	case FullScan:
		return "fullscan"
	case PointGet:
		return "point_get"
	case EqLookup:
		return "eq_lookup"
	case RangeLookup:
		return "range_lookup"
	}
	return "unknown"
}

// fill 拉取下一批行；无更多数据返回 io.EOF。
func (s *RowStream) fill() error {
	// L1 缓存命中：pk 列表分页回表（校验照常在 fetchRows 内）
	if s.ncCached != nil {
		if s.ncOff >= len(s.ncCached) {
			return io.EOF
		}
		end := min(s.ncOff+s.exec.batch, len(s.ncCached))
		err := s.fetchRows(s.ncCached[s.ncOff:end])
		s.ncOff = end
		return err
	}
	switch s.req.Kind {
	case PointGet:
		return s.fillPointGet()
	case RangeLookup:
		return s.fillOrderedRange() // 全局 score 有序流（topk.go：gms 删 Sort 契约）
	case PrefixLookup:
		return s.fillPrefix() // 全局字典序流（prefix.go：同一契约）
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
// （RangeLookup 不走这里——全局有序归但见 topk.go；本路径服务 EqLookup/FullScan。）
func (s *RowStream) fillScatter() error {
	e := s.exec
	// 全扫限流通道（docs/07 §7.4）：首个 fill 获取槽位，EOF/Close 释放
	if s.req.Kind == FullScan && s.fsSem == nil {
		sem, err := e.acquireFullscan(s.ctx)
		if err != nil {
			return err
		}
		s.fsSem = sem
	}
	for {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		// 本组桶游标耗尽 → 推进到下一 slot 组
		if len(s.pending) == 0 {
			slotHi := keycodec.NumSlots
			if s.req.SlotHi > 0 {
				slotHi = s.req.SlotHi
			}
			if s.nextSlot >= slotHi {
				return io.EOF
			}
			if s.nextSlot == 0 && s.req.SlotLo > 0 {
				s.nextSlot = s.req.SlotLo
			}
			from := s.nextSlot
			s.pending = s.buildGroup(from, min(from+e.slotsPerRound, slotHi))
			s.nextSlot = from + e.slotsPerRound
			if len(s.pending) == 0 {
				continue
			}
		}
		// 一轮分页：每个未完成桶拉一页
		cmds := make([]kidb.Cmd, 0, len(s.pending))
		for _, b := range s.pending {
			cmds = append(cmds, s.pageCmd(b))
		}
		results, err := e.readPipeline(s.ctx, cmds)
		if err != nil {
			return fmt.Errorf("exec: scatter page: %w", err)
		}
		var items []candItem
		rest := s.pending[:0]
		for i, b := range s.pending {
			members := utils.Strings(results[i])
			for _, m := range members {
				pk := s.stripCovering(m)
				if s.seen != nil { // 分裂窗口父子桶双读去重
					if _, dup := s.seen[pk]; dup {
						continue
					}
					s.seen[pk] = struct{}{}
				}
				items = append(items, candItem{member: m, pk: pk, val: b.val})
			}
			if len(members) == e.batch { // 满页 = 可能还有
				b.cursor += len(members)
				rest = append(rest, b)
			}
		}
		s.pending = rest
		if len(items) == 0 {
			continue
		}
		if s.ncFP != "" || s.pkOnly {
			for _, it := range items {
				s.ncCollect = append(s.ncCollect, it.pk)
			}
			if s.pkOnly && len(s.ncCollect) > tuning.Get().Exec.L2MaxCollect {
				return errL2Overflow
			}
		}
		if s.pkOnly {
			continue // L2 收集模式：不回表，跑到 EOF
		}
		if err := s.fetchItems(items); err != nil {
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
				for _, b := range s.eqBucketsAt(s16, v) {
					bk := keycodec.EqBucketKey(t.Name, s.req.Index.ID, v, s16, b)
					// L4：热值副本替换源桶读（异 slot 摊开读 QPS，docs/08 §8.4）
					if s.exec.l4 != nil && b == 0 {
						if rep, ok := s.exec.l4.ReplicaFor(s.ctx, t.Name, s.req.Index.ID, keycodec.EscapeValue(v), bk, func(n int) int { return rand.Intn(n) }); ok {
							bk = rep
						}
					}
					if s.exec.telemetry != nil {
						s.exec.telemetry.Sample(s.ctx, bk)
					}
					out = append(out, bucketScan{key: bk, val: v})
				}
			}
		case FullScan:
			for shard := 0; shard < t.EffectiveExpShards(); shard++ {
				out = append(out, bucketScan{key: keycodec.ExpKeyN(t.Name, s16, shard, t.EffectiveExpShards())})
			}
		}
	}
	return out
}

// eqBucketsAt 该 slot 该值的读桶集合（分裂状态感知）。
func (s *RowStream) eqBucketsAt(slot uint16, encVal string) []int {
	if !s.bmHotEq || s.exec.bm == nil {
		return []int{0}
	}
	sh, err := s.exec.bm.Load(s.ctx, s.req.Table.Name, s.req.Index.ID, slot)
	if err != nil {
		return []int{0} // 读不出按默认桶（写路径 CAS 保证不丢数据，读侧退化不多错）
	}
	return sh.ReadBucketsEq(keycodec.EscapeValue(encVal))
}

// pageCmd 生成一桶一页的命令（ZRANGE 偏移分页，带 LIMIT——有界纪律 docs/04 §4.1）。
// （RangeLookup 的分页命令在 topk.go——WITHSCORES + 双向。）
func (s *RowStream) pageCmd(b bucketScan) kidb.Cmd {
	batch := s.exec.batch
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

// fetchItems 候选取行分发：覆盖索引走 member 解码 + 活性校验（跳回表），
// 其余走回表（投影下推 HMGET，docs/04 §4.3）。
func (s *RowStream) fetchItems(items []candItem) error {
	if s.req.Covering {
		return s.fetchCovered(items)
	}
	pks := make([]string, 0, len(items))
	for _, it := range items {
		pks = append(pks, it.pk)
	}
	return s.fetchRows(pks)
}

// needsTTL 报告本请求是否需要行剩余 TTL（仅 _ttl 伪列被**显式投影**时）。
// SELECT *（nil 投影）不物化——剩余 TTL 只在显式 `SELECT _ttl` 时经 PTTL
// 自省（docs/07 §7.1：SELECT * 的 _ttl 列呈 NULL，行缓存不受 PTTL 拖累）。
func (r *Request) needsTTL() bool {
	if r.Projection == nil {
		return false
	}
	for _, c := range r.Projection {
		if strings.EqualFold(c, meta.TTLPseudoColumn) {
			return true
		}
	}
	return false
}

// fetchColumns 回表取列集合：投影 ∪ 谓词列（谓词重校验不可省，docs/04 §4.3）。
// _ttl 伪列不在 Hash 字段里（PTTL 自省），从取列集合剔除。
// nil = 全列（HGETALL）；空非 nil = 零投影且无谓词（EXISTS 活性判定即可）。
func (s *RowStream) fetchColumns() []string {
	if s.req.Projection == nil {
		return nil
	}
	set := map[string]bool{}
	var cols []string
	add := func(c string) {
		if c != "" && !strings.EqualFold(c, s.req.Table.PK) && !set[c] {
			set[c] = true
			cols = append(cols, c)
		}
	}
	for _, c := range s.req.Projection {
		if strings.EqualFold(c, meta.TTLPseudoColumn) {
			continue // 伪列无 Hash 字段（PTTL 自省，fetchRows 内同 pipeline 取）
		}
		add(c)
	}
	if s.req.Pred != nil {
		add(s.req.Pred.Column)
	}
	// 取列集合覆盖全部非主键列时 HGETALL 更省（一次取回免字段列表传输；
	// 主键列本就不在 Hash 字段里——editors.splitRow 把 pk 提出到 key 侧）
	if len(cols) > 0 {
		nonPK := 0
		for _, c := range s.req.Table.Columns {
			if !strings.EqualFold(c.Name, s.req.Table.PK) {
				nonPK++
			}
		}
		if len(cols) >= nonPK {
			return nil
		}
	}
	return cols
}

// fetchCovered 覆盖索引读路径（docs/03 §3.5）：member 自带覆盖列，跳过回表。
// 活性校验 = exp 登记册 ZSCORE（每行必登记，docs/07 §7.2）：score > now 即活
// （+inf 无 TTL；≤now 已过期未清扫；member 缺失 = 已清扫/从未存在）——
// 这是无回表世界"过期行绝不返回"的落实点。同步索引的 member 与行原子一致
// （写 Lua 同事务撤旧建新），覆盖列值即当前值。
func (s *RowStream) fetchCovered(items []candItem) error {
	if len(items) == 0 {
		return nil
	}
	t := s.req.Table
	idx := s.req.Index
	now := s.exec.clock().Unix()
	shards := t.EffectiveExpShards()

	cmds := make([]kidb.Cmd, 0, len(items))
	for _, it := range items {
		slot := keycodec.Slot(keycodec.RowKey(t.Name, it.pk))
		cmds = append(cmds, kidb.Cmd{Name: "ZSCORE", Args: []any{
			keycodec.ExpKeyN(t.Name, slot, keycodec.ExpShardFor(it.pk, shards), shards), it.pk,
		}})
	}
	results, err := s.exec.readPipeline(s.ctx, cmds)
	if err != nil {
		return fmt.Errorf("exec: covered liveness: %w", err)
	}

	var fallback []string // member 解码失败（防御）→ 回表
	for i, it := range items {
		sc, serr := strconv.ParseFloat(fmt.Sprint(results[i]), 64)
		if serr != nil || sc <= float64(now) {
			continue // 已过期/已清扫
		}
		covers := rowcodec.MemberCovers(it.member, true)
		if covers == nil || len(covers) != len(idx.Covering) {
			fallback = append(fallback, it.pk)
			continue
		}
		raw := make(map[string]string, len(covers)+1)
		raw[idx.Columns[0]] = it.val
		for j, c := range idx.Covering {
			raw[c] = covers[j]
		}
		if !s.req.Pred.Match(raw) {
			if s.exec.m != nil {
				s.exec.m.RowsFiltered.Inc()
			}
			continue
		}
		s.rows = append(s.rows, rowcodec.DecodeRowCols(t, it.pk, raw, s.req.Projection))
	}
	if len(fallback) > 0 {
		return s.fetchRows(fallback)
	}
	return nil
}

// fetchRows 批量回表（pipeline，512 批；投影子集用 HMGET，零投影用 EXISTS）→
// 空行跳过 → 谓词校验 → 解码入队。
// 这是"回表校验"纪律的落实点（docs/04 §4.3）：一切索引路径的输出都经此过滤。
// 谓词为白名单形态且开启 Pushdown 时，校验在服务端 Lua 完成（网络只传命中行）。
// 行级近缓存开启时：命中行零 RTT；未命中行 HGETALL+PTTL 同 pipeline 取回并填充
// （条目 TTL ≤ 行剩余 TTL——过期行绝不返回；更新陈旧窗口语义见 docs/08 §8.4）。
func (s *RowStream) fetchRows(pks []string) error {
	if s.req.Pushdown && s.req.Pred != nil && s.req.Pred.pushdownable() && s.exec.reg != nil && !s.req.needsTTL() {
		return s.fetchRowsPushdown(s.ctx, pks)
	}
	t := s.req.Table
	cols := s.fetchColumns()
	needTTL := s.req.needsTTL()
	rc := s.exec.rowCache
	if needTTL {
		rc = nil // 缓存条目不携带行剩余 TTL（填充时点的 min 值会漂移）——绕过
	}
	for i := 0; i < len(pks); i += s.exec.batch {
		batch := pks[i:min(i+s.exec.batch, len(pks))]
		raws := make([]map[string]string, len(batch))

		// 行缓存预取：命中直接可用（谓词校验/解码照走——缓存只是免 RTT）
		var miss []int
		for j, pk := range batch {
			if rc != nil {
				if f, ok := rc.Get(keycodec.RowKey(t.Name, pk)); ok {
					raws[j] = f
					continue
				}
			}
			miss = append(miss, j)
		}

		if len(miss) > 0 {
			var cmds []kidb.Cmd
			for _, j := range miss {
				rk := keycodec.RowKey(t.Name, batch[j])
				// 需要剩余 TTL 时同行附 PTTL（同 pipeline 零额外 RTT）
				withPTTL := rc != nil || needTTL
				switch {
				case rc != nil:
					// 行缓存开启：取全行 + PTTL（填充寿命 = min(条目默认, 行剩余)）
					cmds = append(cmds, kidb.Cmd{Name: "HGETALL", Args: []any{rk}},
						kidb.Cmd{Name: "PTTL", Args: []any{rk}})
				case cols == nil:
					cmds = append(cmds, kidb.Cmd{Name: "HGETALL", Args: []any{rk}})
				case len(cols) == 0:
					cmds = append(cmds, kidb.Cmd{Name: "EXISTS", Args: []any{rk}})
				default:
					args := make([]any, 0, len(cols)+1)
					args = append(args, rk)
					for _, c := range cols {
						args = append(args, c)
					}
					cmds = append(cmds, kidb.Cmd{Name: "HMGET", Args: args})
				}
				if withPTTL && rc == nil {
					cmds = append(cmds, kidb.Cmd{Name: "PTTL", Args: []any{rk}})
				}
			}
			results, err := s.exec.readPipeline(s.ctx, cmds)
			if err != nil {
				return fmt.Errorf("exec: fetch rows: %w", err)
			}
			ri := 0
			for _, j := range miss {
				withPTTL := rc != nil || needTTL
				readTTL := func(raw map[string]string) int64 {
					if !withPTTL {
						return -1
					}
					pttlMs, _ := strconv.ParseInt(fmt.Sprint(results[ri]), 10, 64)
					ri++
					// 剩余秒（上取整：亚秒 TTL 不呈 0 混淆"无 TTL"语义；-1 永不过期原样）
					if raw != nil && pttlMs > 0 {
						raw[meta.TTLPseudoColumn] = strconv.FormatInt((pttlMs+999)/1000, 10)
					} else if raw != nil {
						raw[meta.TTLPseudoColumn] = "-1"
					}
					return pttlMs
				}
				switch {
				case rc != nil:
					raw, _ := utils.StringMap(results[ri])
					ri++
					if len(raw) == 0 {
						continue // 空 Hash = 行过期/不存在
					}
					pttlMs := readTTL(raw)
					rc.Add(keycodec.RowKey(t.Name, batch[j]), raw, time.Duration(pttlMs)*time.Millisecond) // 条目寿命 ≤ 行剩余 TTL（过期行绝不返回）
					raws[j] = raw
				case cols == nil:
					raw, _ := utils.StringMap(results[ri])
					ri++
					if len(raw) == 0 {
						continue
					}
					readTTL(raw)
					raws[j] = raw
				case len(cols) == 0:
					if fmt.Sprint(results[ri]) == "1" {
						raws[j] = map[string]string{}
					}
					ri++
					if raws[j] != nil {
						readTTL(raws[j])
					}
				default:
					raws[j] = hmgetMap(cols, results[ri])
					ri++
					readTTL(raws[j])
				}
			}
		}

		for j, pk := range batch {
			raw := raws[j]
			if raw == nil {
				continue // 行不存在/过期/缓存未命中填充为空
			}
			if !s.req.Pred.Match(raw) {
				if s.exec.m != nil {
					s.exec.m.RowsFiltered.Inc() // 回表校验拦截量（docs/10 §10.3）
				}
				continue
			}
			s.rows = append(s.rows, rowcodec.DecodeRowCols(t, pk, raw, s.req.Projection))
		}
	}
	return nil
}

// hmgetMap 把 HMGET 的位置式回复（nil = 字段缺失）装配为字段映射。
func hmgetMap(cols []string, res any) map[string]string {
	raw := make(map[string]string, len(cols))
	vals, ok := res.([]any)
	if !ok {
		return raw
	}
	for k, v := range vals {
		if k >= len(cols) || v == nil {
			continue
		}
		raw[cols[k]] = fmt.Sprint(v)
	}
	return raw
}

// stripCovering 提取桶 member 的 pk（schema 感知：覆盖列为 msgp 数组编码，
// 由调用方按索引定义传入 hasCovering，docs/03 §3.5）。
func (s *RowStream) stripCovering(member string) string {
	if s.req.Index != nil && len(s.req.Index.Covering) > 0 {
		return rowcodec.MemberPK(member, true)
	}
	return member
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
	results, err := e.readPipeline(ctx, cmds)
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
