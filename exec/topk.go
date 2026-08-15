package exec

import (
	"io"
	"math"
	"strconv"

	pq "gopkg.in/dnaeon/go-priorityqueue.v1"

	"kidb"
	"kidb/keycodec"
	"kidb/meta"
)

// topk.go：RangeLookup 的全局 score 有序流（docs/04 §4.1：ORDER BY num LIMIT k
// = 沿 score 有序桶从端点 k 路归并 top-k）。
//
// 为什么必须始终有序（gms 分析器契约，sql/analyzer/replace_sort.go）：
// ORDER BY 列与索引表达式前缀匹配时，gms 直接删除 Sort 节点——
// ASC 情形不咨询任何接口，DESC/全表情形仅查 OrderedIndex（engine.Index 已实现）。
// 等价于 gms 约定"索引扫描必须按索引列升序产出"。KiDB 的范围桶按 slot 散布，
// 故 RangeLookup 一律走本归并器产出全局 score 序（ASC；Request.Desc 时反向），
// 排序正确性由构造保证，不依赖对分析器决策的探测。
//
// 归并形状：种子轮对全部 slot 的覆盖桶端点各取 1 成员（WITHSCORES）建堆
// （16384 路散取与 COUNT(*) 的 Σ ZCOUNT 同形，docs/04 §4.1）；
// 弹出赢家后按需补页（pageK 成员/次）；每批候选照常回表校验（docs/04 §4.3）。
// LIMIT 早停由引擎 Limit 节点停止消费自然达成——归并器只被拉取，从不预取全量。

const (
	// topkRefillPage 归并补页大小（每 way 每次补页取回的成员数）。
	// 权衡：1 = 每弹出一个成员就多一条补页命令；过大 = 全表遍历场景的瞬时缓冲
	// （16384 桶 × pageK 成员）膨胀。16 使命令放大 ≤1/16、缓冲上限 ~6MB。
	topkRefillPage = 16
)

// topkItem 堆条目（comparable：member 在单桶内唯一，way 区分跨桶同 member 的
// 分裂窗口双读——去重由 RowStream.seen 在回表前完成）。
type topkItem struct {
	member string
	way    int
}

// mergeWay 一条归并路 = 一个桶的游标。
type mergeWay struct {
	key    string // 桶 key
	cursor int    // 已取成员数（ZRANGEBYSCORE LIMIT 的 offset）
	inHeap int    // 堆里未消费的本桶成员数
	done   bool   // 桶已穷尽（最后一次补页未满载）
}

// orderedMerger 单 score 区间的 k 路归并器（多区间由 RowStream 逐区间驱动：
// 区间互不重叠时按 Lo 顺序拼接即全局有序——translate 已排序；
// gms 只在区间不重叠时删 Sort，docs/04 §4.3）。
type orderedMerger struct {
	s     *RowStream
	r     RangeBound
	pq    *pq.PriorityQueue[topkItem, float64]
	ways  []mergeWay
	desc  bool
	seeded bool
	empty bool // 本区间确定无更多成员（堆空且无待补页）
}

// newOrderedMerger 构造（不发起 IO；首个 fill 时种子）。
func newOrderedMerger(s *RowStream, r RangeBound, desc bool) *orderedMerger {
	kind := pq.MinHeap
	if desc {
		kind = pq.MaxHeap
	}
	return &orderedMerger{s: s, r: r, desc: desc, pq: pq.New[topkItem, float64](kind)}
}

// fillOrderedRange 驱动归并：区间耗尽推进下一区间，全部耗尽 io.EOF。
func (s *RowStream) fillOrderedRange() error {
	for {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		if s.om == nil {
			if s.rangeIdx >= len(s.req.Ranges) {
				return io.EOF
			}
			s.om = newOrderedMerger(s, s.req.Ranges[s.rangeIdx], s.req.Desc)
		}
		err := s.om.step()
		if err == io.EOF {
			s.om = nil
			s.rangeIdx++
			continue
		}
		return err
	}
}

// step 产出一批已校验行；本区间耗尽返回 io.EOF。
func (om *orderedMerger) step() error {
	s := om.s
	e := s.exec
	if !om.seeded {
		if err := om.seed(); err != nil {
			return err
		}
		om.seeded = true
	}

	// 弹出批：≤batch 个候选（堆空且无待补页 = 本区间耗尽）
	type scored struct {
		it    topkItem
		score float64
	}
	var cand []scored
	var refill []int
	for !om.pq.IsEmpty() && len(cand) < e.batch {
		it := om.pq.Get()
		cand = append(cand, scored{it.Value, it.Priority})
		w := &om.ways[it.Value.way]
		w.inHeap--
		if w.inHeap == 0 && !w.done {
			refill = append(refill, it.Value.way)
		}
	}
	if len(cand) == 0 && len(refill) == 0 {
		return io.EOF
	}

	// 补页（与回表分离为两轮 pipeline——批 512 下轮数可忽略，换取结构简单）
	if len(refill) > 0 {
		if err := om.refillWays(refill); err != nil {
			return err
		}
	}
	if len(cand) == 0 {
		return nil // 纯补页轮（极端：上批全部命中同一桶），下轮再弹
	}

	// 回表/覆盖分发（去重在取 pk 时完成，分裂窗口/多区间重读共用 s.seen）；
	// val = score 反编码（覆盖路径重建索引列字段用，docs/03 §3.5）
	col, _ := s.req.Table.Column(s.req.Index.Columns[0])
	items := make([]candItem, 0, len(cand))
	for _, c := range cand {
		pk := s.stripCovering(c.it.member)
		if s.seen != nil {
			if _, dup := s.seen[pk]; dup {
				continue
			}
			s.seen[pk] = struct{}{}
		}
		items = append(items, candItem{member: c.it.member, pk: pk, val: scoreEnc(col.Type, c.score)})
	}
	if len(items) == 0 {
		return nil
	}
	return s.fetchItems(items)
}

// scoreEnc score 反编码为列存储形态（float64→int64 在 ≤2^53 精确——
// DDL 限定范围索引列界内；float 最短表示往返无损）。
func scoreEnc(ct meta.ColumnType, score float64) string {
	switch ct {
	case meta.ColInt, meta.ColTimestamp:
		return strconv.FormatInt(int64(score), 10)
	case meta.ColFloat:
		return strconv.FormatFloat(score, 'g', -1, 64)
	}
	return ""
}

// seed 种子轮：全部 slot 的覆盖桶端点各取 1 成员（WITHSCORES）。
func (om *orderedMerger) seed() error {
	s := om.s
	e := s.exec
	t := s.req.Table

	om.ways = om.ways[:0]
	slotLo, slotHi := 0, keycodec.NumSlots
	if s.req.SlotHi > 0 {
		slotHi = s.req.SlotHi
	}
	if s.req.SlotLo > 0 {
		slotLo = s.req.SlotLo
	}
	for slot := slotLo; slot < slotHi; slot++ {
		for _, b := range s.rangeBucketsAtFor(uint16(slot), om.r) {
			om.ways = append(om.ways, mergeWay{key: keycodec.RangeBucketKey(t.Name, s.req.Index.ID, uint16(slot), b)})
		}
	}

	cmds := make([]kidb.Cmd, 0, len(om.ways))
	for _, w := range om.ways {
		cmds = append(cmds, om.pageCmd(w.key, 0, 1))
	}
	results, err := e.readPipeline(s.ctx, cmds)
	if err != nil {
		return err
	}
	for i, res := range results {
		score, member, ok := om.parseWithScores(res)
		if !ok {
			om.ways[i].done = true
			continue
		}
		w := &om.ways[i]
		w.cursor = 1
		w.inHeap = 1
		om.pq.Put(topkItem{member: member, way: i}, score)
		if e.telemetry != nil {
			e.telemetry.Sample(s.ctx, w.key) // 只采样有成员的桶（热在数据里）
		}
	}
	return nil
}

// refillWays 批量补页：每条 way 取 pageK 成员，全部入堆。
func (om *orderedMerger) refillWays(idxs []int) error {
	s := om.s
	cmds := make([]kidb.Cmd, 0, len(idxs))
	for _, wi := range idxs {
		w := &om.ways[wi]
		cmds = append(cmds, om.pageCmd(w.key, w.cursor, topkRefillPage))
	}
	results, err := s.exec.readPipeline(s.ctx, cmds)
	if err != nil {
		return err
	}
	for j, res := range results {
		w := &om.ways[idxs[j]]
		members := asStrings(res)
		n := len(members) / 2 // WITHSCORES 扁平对
		if n == 0 {
			w.done = true
			continue
		}
		for k := 0; k < n; k++ {
			score, perr := strconv.ParseFloat(members[2*k+1], 64)
			if perr != nil || math.IsNaN(score) {
				continue // 脏 score 防御（正常路径不会到达：DDL 限定数值列）
			}
			om.pq.Put(topkItem{member: members[2*k], way: idxs[j]}, score)
			w.inHeap++
		}
		w.cursor += n
		if n < topkRefillPage {
			w.done = true
		}
	}
	return nil
}

// pageCmd 单桶分页命令（ASC：ZRANGEBYSCORE lo hi；DESC：ZREVRANGEBYSCORE hi lo）。
func (om *orderedMerger) pageCmd(key string, off, count int) kidb.Cmd {
	if om.desc {
		return kidb.Cmd{Name: "ZREVRANGEBYSCORE", Args: []any{
			key, rangeBound(om.r.Hi, om.r.HiOpen), rangeBound(om.r.Lo, om.r.LoOpen),
			"WITHSCORES", "LIMIT", off, count,
		}}
	}
	return kidb.Cmd{Name: "ZRANGEBYSCORE", Args: []any{
		key, rangeBound(om.r.Lo, om.r.LoOpen), rangeBound(om.r.Hi, om.r.HiOpen),
		"WITHSCORES", "LIMIT", off, count,
	}}
}

// parseWithScores 解析 WITHSCORES 扁平返回的首个 (member, score) 对。
func (om *orderedMerger) parseWithScores(res any) (float64, string, bool) {
	arr := asStrings(res)
	if len(arr) < 2 {
		return 0, "", false
	}
	score, err := strconv.ParseFloat(arr[1], 64)
	if err != nil || math.IsNaN(score) {
		return 0, "", false
	}
	return score, arr[0], true
}

// rangeBucketsAtFor 指定 slot 覆盖 [r.Lo,r.Hi] 的桶集合（rangeBucketsAt 的
// 显式区间版——归并器按自身区间推进，与 RowStream.rangeIdx 解耦）。
func (s *RowStream) rangeBucketsAtFor(slot uint16, r RangeBound) []int {
	if !s.bmHotRange || s.exec.bm == nil {
		return []int{0}
	}
	sh, err := s.exec.bm.Load(s.ctx, s.req.Table.Name, s.req.Index.ID, slot)
	if err != nil {
		return []int{0}
	}
	lo, hi := r.Lo, r.Hi
	if r.LoOpen {
		lo = math.Nextafter(lo, math.Inf(1))
	}
	if r.HiOpen {
		hi = math.Nextafter(hi, math.Inf(-1))
	}
	return sh.ReadBucketsRange(lo, hi)
}
