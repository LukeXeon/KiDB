package exec

import (
	"io"
	"strings"

	"kidb/keycodec"
	"kidb/kv"
	"kidb/tuning"
	"kidb/utils"
)

// prefix.go：前缀搜索 LIKE 'abc%' 的字典序副本查询路径（docs/04 §4.5）：
// 各 slot 字典序副本桶 ZRANGEBYLEX [prefix [prefix\xff 分页，member = 值+\x00+pk，
// k 路归并产出全局字典序（字符串堆）。
//
// 为什么必须归并（与 topk.go 同一契约）：gms replace_sort.go 对 ORDER BY 索引列
// ASC 直接删 Sort（不咨询接口）——`WHERE city LIKE 'a%' ORDER BY city` 的产出
// 必须全局字典序。DESC 由 eq 索引 Reversible()=false 挡回引擎层 sort。
//
// 值编码假设（docs/04 §4.5 既有声明）：字符串列不含 \xff 字节（UTF-8 天然满足），
// 不含 \x00 的极端值由回表校验的 HasPrefix 重判兜底（member 截断只影响该值自身
// 的分桶序，不产生错行）。

// lexItem 归并堆条目（member 全序即（值, pk）序——member 比较即字典序比较）。
type lexItem struct {
	member string
	way    int
}

// lexMerger 字典序副本的 k 路归并器（ds 泛型堆，member 为优先级 = 小顶堆字典序）。
type lexMerger struct {
	s      *RowStream
	heap   *utils.PriorityQueue[lexItem, string]
	ways   []mergeWay
	seeded bool
}

// fillPrefix 驱动字典序归并（单区间：[LexLo, LexHi) 无多区间概念）。
func (s *RowStream) fillPrefix() error {
	if s.lm == nil {
		s.lm = &lexMerger{s: s, heap: utils.NewMinPriorityQueue[lexItem, string]()}
	}
	return s.lm.step()
}

// step 产出一批已校验行；耗尽返回 io.EOF。
func (lm *lexMerger) step() error {
	s := lm.s
	e := s.exec
	if !lm.seeded {
		if err := lm.seed(); err != nil {
			return err
		}
		lm.seeded = true
	}

	var cand []lexItem
	var refill []int
	for lm.heap.Len() > 0 && len(cand) < e.batch {
		it, _ := lm.heap.Pop()
		cand = append(cand, it)
		w := &lm.ways[it.way]
		w.inHeap--
		if w.inHeap == 0 && !w.done {
			refill = append(refill, it.way)
		}
	}
	if len(cand) == 0 && len(refill) == 0 {
		return io.EOF
	}
	if len(refill) > 0 {
		if err := lm.refillWays(refill); err != nil {
			return err
		}
	}
	if len(cand) == 0 {
		return nil
	}

	// member = 值+\x00+pk → 提取（回表校验在 fetchItems 内重做 HasPrefix）
	items := make([]candItem, 0, len(cand))
	for _, c := range cand {
		cut := strings.IndexByte(c.member, 0)
		if cut <= 0 {
			continue // 畸形 member 防御（无 \x00 分隔）
		}
		pk := c.member[cut+1:]
		if s.seen != nil {
			if s.seen.Has(pk) {
				continue
			}
			s.seen.Add(pk)
		}
		items = append(items, candItem{member: c.member, pk: pk, val: c.member[:cut]})
	}
	if len(items) == 0 {
		return nil
	}
	return s.fetchItems(items)
}

// seed 种子轮：全 slot 字典序副本桶端点各取 1 成员。
func (lm *lexMerger) seed() error {
	s := lm.s
	e := s.exec
	t := s.req.Table

	lm.ways = lm.ways[:0]
	for _, b := range s.lexBuckets() {
		lm.ways = append(lm.ways, mergeWay{key: keycodec.LexBucketKey(t.Name, s.req.Index.ID, b)})
	}

	cmds := make([]kv.Cmd, 0, len(lm.ways))
	for _, w := range lm.ways {
		cmds = append(cmds, lm.pageCmd(w.key, 0, 1))
	}
	results, err := e.readPipeline(s.ctx, cmds)
	if err != nil {
		return err
	}
	for i, res := range results {
		members := utils.Strings(res)
		if len(members) == 0 {
			lm.ways[i].done = true
			continue
		}
		w := &lm.ways[i]
		w.cursor = 1
		w.inHeap = 1
		lm.heap.Push(lexItem{member: members[0], way: i}, members[0])
		if e.telemetry != nil {
			e.telemetry.Sample(s.ctx, w.key)
		}
	}
	return nil
}

// refillWays 批量补页。
func (lm *lexMerger) refillWays(idxs []int) error {
	s := lm.s
	cmds := make([]kv.Cmd, 0, len(idxs))
	for _, wi := range idxs {
		w := &lm.ways[wi]
		cmds = append(cmds, lm.pageCmd(w.key, w.cursor, tuning.Get().Exec.LexRefillPage))
	}
	results, err := s.exec.readPipeline(s.ctx, cmds)
	if err != nil {
		return err
	}
	for j, res := range results {
		w := &lm.ways[idxs[j]]
		members := utils.Strings(res)
		if len(members) == 0 {
			w.done = true
			continue
		}
		for _, m := range members {
			lm.heap.Push(lexItem{member: m, way: idxs[j]}, m)
			w.inHeap++
		}
		w.cursor += len(members)
		if len(members) < tuning.Get().Exec.LexRefillPage {
			w.done = true
		}
	}
	return nil
}

// pageCmd 单桶 ZRANGEBYLEX 分页（有界纪律）。
func (lm *lexMerger) pageCmd(key string, off, count int) kv.Cmd {
	return kv.Cmd{Name: "ZRANGEBYLEX", Args: []any{
		key, "[" + lm.s.req.LexLo, "[" + lm.s.req.LexHi, "LIMIT", off, count,
	}}
}

// lexBucketsAt 该 slot 字典序副本读桶集合（分裂感知：bm 以 Eq["l"] 伪条目
// 承载副本分裂状态——与写路径 txguard 的 eqWriteSet(sh,"l",pk) 同一约定；
// 当前控制器不触发副本分裂，读集合恒为 [0]，本函数是将来启用的挂点）。
// 稀疏门控与等值一致：注册表无 "l" 热点标记时零额外读（docs/03 §3.1）。
func (s *RowStream) lexBuckets() []int {
	if !s.bmHotLex || s.exec.bm == nil {
		return []int{0}
	}
	d, err := s.exec.bm.Load(s.ctx, s.req.Table.Name, s.req.Index.ID)
	if err != nil {
		return []int{0}
	}
	return d.ReadBucketsEq("l")
}
