package controller

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cespare/xxhash/v2"

	"kidb"
	"kidb/bucketmap"
	"kidb/keycodec"
	"kidb/script"
)

// split.go：桶分裂协议驱动（docs/08 §8.3）：
//   CAS SPLITTING → 分批搬迁（split_migrate.lua，每批 500）→ CAS DRAINING
//   → UNLINK 父桶 → CAS ACTIVE(子桶)。
// 全部状态持久于 bm 分片：任何一步失败/宕机，下一次 Split 调用从中间态续作。

// Splitter 是分裂协议执行器。
type Splitter struct {
	cli   kidb.Client
	reg   *script.Registry
	bm    *bucketmap.Store
	batch int
}

// NewSplitter 构造（batch 默认 500，docs/08 §8.3）。
func NewSplitter(cli kidb.Client, reg *script.Registry, bm *bucketmap.Store) *Splitter {
	return &Splitter{cli: cli, reg: reg, bm: bm, batch: 500}
}

// maxStepRetries 是单步 CAS 冲突的重试上限（断点续作保证重试收敛）。
const maxStepRetries = 8

// SplitEq 等值桶分裂：该 (值, slot) 桶的成员数翻倍（2n 子桶，xxhash 取模放置）。
// 可断点续作：Split 中间态持久化，重复调用从中间态继续。
func (s *Splitter) SplitEq(ctx context.Context, table, idxID, encVal string, slot uint16) error {
	key := bucketmap.Key(table, idxID, slot)

	for attempt := 0; attempt < maxStepRetries; attempt++ {
		sh, err := s.bm.LoadFresh(ctx, table, idxID, slot)
		if err != nil {
			return err
		}
		e := sh.Eq[encVal]
		if e == nil {
			e = &bucketmap.EqEntry{Buckets: []int{0}}
		}

		// 第 1 步：CAS 置 SPLITTING（分配子桶下标）
		if e.Split == nil {
			n := len(e.Buckets)
			children := make([]int, 2*n)
			for i := range children {
				children[i] = sh.Next + i
			}
			if _, err := s.bm.CAS(ctx, key, sh.Version, "next", sh.Next+2*n); err != nil {
				continue
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID, slot)
			if err != nil {
				return err
			}
			entry := &bucketmap.EqEntry{
				Buckets: e.Buckets,
				Split:   &bucketmap.SplitInfo{State: bucketmap.Splitting, Parents: e.Buckets, Children: children},
			}
			if _, err := s.bm.CAS(ctx, key, sh2.Version, "e:"+encVal, entry); err != nil {
				continue
			}
			e = entry
		}

		// 第 2 步：分批搬迁
		if e.Split.State == bucketmap.Splitting {
			if err := s.migrateEq(ctx, table, idxID, encVal, slot, e.Split); err != nil {
				return err
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID, slot)
			if err != nil {
				return err
			}
			e2 := sh.Eq[encVal]
			if e2 == nil || e2.Split == nil {
				continue // 中间态被并发改写 → 重读收敛
			}
			e2.Split.State = bucketmap.Draining
			if _, err := s.bm.CAS(ctx, key, sh2.Version, "e:"+encVal, e2); err != nil {
				continue
			}
			e = e2
		}

		// 第 3 步：UNLINK 父桶 → CAS ACTIVE(子桶)
		if e.Split.State == bucketmap.Draining {
			for _, pIdx := range e.Split.Parents {
				if _, err := s.cli.Do(ctx, "UNLINK", keycodec.EqBucketKey(table, idxID, encVal, slot, pIdx)); err != nil {
					return err
				}
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID, slot)
			if err != nil {
				return err
			}
			final := &bucketmap.EqEntry{Buckets: e.Split.Children}
			if _, err := s.bm.CAS(ctx, key, sh2.Version, "e:"+encVal, final); err != nil {
				continue
			}
			return s.bm.RegisterHot(ctx, table, idxID, encVal)
		}
	}
	return fmt.Errorf("%w: SplitEq %s/%s/%s slot %d", kidb.ErrStaleMetadata, table, idxID, encVal, slot)
}

// migrateEq 搬迁：每父桶循环取首页（搬迁即删，首页恒为剩余头部）→
// 按 xxhash(pk) % len(children) 选子桶 → split_migrate 批迁移。
func (s *Splitter) migrateEq(ctx context.Context, table, idxID, encVal string, slot uint16, sp *bucketmap.SplitInfo) error {
	sm, _ := s.reg.Get("split_migrate")
	childKeys := make([]string, 0, len(sp.Children))
	for _, cIdx := range sp.Children {
		childKeys = append(childKeys, keycodec.EqBucketKey(table, idxID, encVal, slot, cIdx))
	}
	for _, pIdx := range sp.Parents {
		parentKey := keycodec.EqBucketKey(table, idxID, encVal, slot, pIdx)
		for {
			res, err := s.cli.Do(ctx, "ZRANGE", parentKey, 0, s.batch-1)
			if err != nil {
				return err
			}
			members := asStrings(res)
			if len(members) == 0 {
				break
			}
			if err := s.migrateBatch(ctx, sm, table, slot, parentKey, childKeys, members, func(member string) string {
				return keycodec.EqBucketKey(table, idxID, encVal, slot, sp.Children[subOf(member, len(sp.Children))])
			}); err != nil {
				return err
			}
			if len(members) < s.batch {
				break
			}
		}
	}
	return nil
}

// migrateBatch 一批成员的搬迁（同 slot Lua 原子）。
func (s *Splitter) migrateBatch(ctx context.Context, sm *script.Script, table string, slot uint16,
	parentKey string, childKeys []string, members []string, targetOf func(member string) string) error {

	childIdx := map[string]int{}
	for i, k := range childKeys {
		childIdx[k] = i + 1 // 1 起（KEYS[2..C+1]）
	}
	var rowkeys []string
	rowIdx := map[string]int{}
	refRow := func(rk string) int {
		if i, ok := rowIdx[rk]; ok {
			return i
		}
		rowkeys = append(rowkeys, rk)
		rowIdx[rk] = len(rowkeys)
		return len(rowkeys)
	}

	argv := []any{strconv.Itoa(len(childKeys)), strconv.Itoa(len(members))}
	for _, m := range members {
		pk := stripCovering(m)
		target := targetOf(m)
		score := "0"
		argv = append(argv, m, strconv.Itoa(childIdx[target]), score, strconv.Itoa(refRow(keycodec.RowKey(table, pk))))
	}
	keys := append([]string{parentKey}, childKeys...)
	keys = append(keys, rowkeys...)
	_, err := s.cli.Eval(ctx, sm, keys, argv...)
	return err
}

// SplitRange 范围桶分裂：指定桶按采样中位数裂为 [lo,mid) [mid,hi)。
func (s *Splitter) SplitRange(ctx context.Context, table, idxID string, slot uint16, bucketIdx int) error {
	key := bucketmap.Key(table, idxID, slot)

	for attempt := 0; attempt < maxStepRetries; attempt++ {
		sh, err := s.bm.LoadFresh(ctx, table, idxID, slot)
		if err != nil {
			return err
		}
		var rbPos int = -1
		for i, rb := range sh.Ranges {
			if rb.Idx == bucketIdx {
				rbPos = i
				break
			}
		}
		if rbPos < 0 {
			return fmt.Errorf("controller: range bucket %d not found", bucketIdx)
		}
		rb := sh.Ranges[rbPos]

		if len(rb.Children) == 0 {
			// 采样中位数（docs/08 §8.2：搬迁时采样估算）
			mid, err := s.sampleMedian(ctx, table, idxID, slot, bucketIdx)
			if err != nil {
				return err
			}
			children := []bucketmap.RangeBucket{
				{Idx: sh.Next, Lo: rb.Lo, Hi: bucketmap.FormatBound(mid), State: bucketmap.Active},
				{Idx: sh.Next + 1, Lo: bucketmap.FormatBound(mid), Hi: rb.Hi, State: bucketmap.Active},
			}
			if _, err := s.bm.CAS(ctx, key, sh.Version, "next", sh.Next+2); err != nil {
				continue
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID, slot)
			if err != nil {
				return err
			}
			sh2.Ranges[rbPos].Children = children
			sh2.Ranges[rbPos].State = bucketmap.Splitting
			if _, err := s.bm.CAS(ctx, key, sh2.Version, "r", sh2.Ranges); err != nil {
				continue
			}
			rb = sh2.Ranges[rbPos]
		}

		if rb.State == bucketmap.Splitting {
			if err := s.migrateRange(ctx, table, idxID, slot, rb); err != nil {
				return err
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID, slot)
			if err != nil {
				return err
			}
			sh2.Ranges[rbPos].State = bucketmap.Draining
			if _, err := s.bm.CAS(ctx, key, sh2.Version, "r", sh2.Ranges); err != nil {
				continue
			}
			rb = sh2.Ranges[rbPos]
		}

		if rb.State == bucketmap.Draining {
			if _, err := s.cli.Do(ctx, "UNLINK", keycodec.RangeBucketKey(table, idxID, slot, rb.Idx)); err != nil {
				return err
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID, slot)
			if err != nil {
				return err
			}
			// 父桶位置替换为两个 ACTIVE 子桶
			var nr []bucketmap.RangeBucket
			for _, x := range sh2.Ranges {
				if x.Idx == bucketIdx {
					nr = append(nr, rb.Children...)
				} else {
					nr = append(nr, x)
				}
			}
			if _, err := s.bm.CAS(ctx, key, sh2.Version, "r", nr); err != nil {
				continue
			}
			return s.bm.RegisterHot(ctx, table, idxID, "@range")
		}
	}
	return fmt.Errorf("%w: SplitRange %s/%s slot %d", kidb.ErrStaleMetadata, table, idxID, slot)
}

// sampleMedian 采样估算区间中位数（首页 WITHSCORES，docs/08 §8.2）。
func (s *Splitter) sampleMedian(ctx context.Context, table, idxID string, slot uint16, bucketIdx int) (float64, error) {
	res, err := s.cli.Do(ctx, "ZRANGE", keycodec.RangeBucketKey(table, idxID, slot, bucketIdx), 0, 511, "WITHSCORES")
	if err != nil {
		return 0, err
	}
	arr := asAnySlice(res)
	var scores []float64
	for i := 1; i < len(arr); i += 2 {
		f, err := strconv.ParseFloat(fmt.Sprint(arr[i]), 64)
		if err == nil {
			scores = append(scores, f)
		}
	}
	if len(scores) == 0 {
		return 0, fmt.Errorf("controller: range bucket empty, nothing to split")
	}
	return scores[len(scores)/2], nil
}

// migrateRange 范围桶搬迁：按子桶区间边界选边。
func (s *Splitter) migrateRange(ctx context.Context, table, idxID string, slot uint16, rb bucketmap.RangeBucket) error {
	sm, _ := s.reg.Get("split_migrate")
	parentKey := keycodec.RangeBucketKey(table, idxID, slot, rb.Idx)
	childKeys := []string{
		keycodec.RangeBucketKey(table, idxID, slot, rb.Children[0].Idx),
		keycodec.RangeBucketKey(table, idxID, slot, rb.Children[1].Idx),
	}
	mid := boundOf(rb.Children[1].Lo)
	for {
		res, err := s.cli.Do(ctx, "ZRANGE", parentKey, 0, s.batch-1, "WITHSCORES")
		if err != nil {
			return err
		}
		arr := asAnySlice(res)
		if len(arr) == 0 {
			return nil
		}
		var members []string
		targets := map[string]string{} // member → childKey
		scores := map[string]string{}
		for i := 0; i+1 < len(arr); i += 2 {
			m := fmt.Sprint(arr[i])
			sc := fmt.Sprint(arr[i+1])
			f, _ := strconv.ParseFloat(sc, 64)
			members = append(members, m)
			scores[m] = sc
			if f < mid {
				targets[m] = childKeys[0]
			} else {
				targets[m] = childKeys[1]
			}
		}
		// 范围桶 score 必须保留——不能用 migrateBatch 的固定 score=0，直接组包：
		if err := s.migrateBatchScored(ctx, sm, table, parentKey, childKeys, members, targets, scores); err != nil {
			return err
		}
		if len(members) < s.batch {
			return nil
		}
	}
}

// migrateBatchScored 带 score 的搬迁批（范围桶专用）。
func (s *Splitter) migrateBatchScored(ctx context.Context, sm *script.Script, table, parentKey string,
	childKeys, members []string, targets map[string]string, scores map[string]string) error {

	childIdx := map[string]int{}
	for i, k := range childKeys {
		childIdx[k] = i + 1
	}
	var rowkeys []string
	rowIdx := map[string]int{}
	refRow := func(rk string) int {
		if i, ok := rowIdx[rk]; ok {
			return i
		}
		rowkeys = append(rowkeys, rk)
		rowIdx[rk] = len(rowkeys)
		return len(rowkeys)
	}
	argv := []any{strconv.Itoa(len(childKeys)), strconv.Itoa(len(members))}
	for _, m := range members {
		pk := stripCovering(m)
		argv = append(argv, m, strconv.Itoa(childIdx[targets[m]]), scores[m], strconv.Itoa(refRow(keycodec.RowKey(table, pk))))
	}
	keys := append([]string{parentKey}, childKeys...)
	keys = append(keys, rowkeys...)
	_, err := s.cli.Eval(ctx, sm, keys, argv...)
	return err
}

// subOf 子桶放置（xxhash64 取模）。
func subOf(member string, n int) int {
	return int(xxhash.Sum64String(stripCovering(member)) % uint64(n))
}

func stripCovering(member string) string {
	for i := 0; i < len(member); i++ {
		if member[i] == '|' {
			return member[:i]
		}
	}
	return member
}

func boundOf(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
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

func asAnySlice(res any) []any {
	switch v := res.(type) {
	case []any:
		return v
	default:
		return nil
	}
}

// ==== 合并（分裂的镜像协议，docs/08 §8.3 末段）====

// MergeEq 等值桶合并：当前 2n 子桶并为 n 个（持续低基数的回收路径）。
// 镜像状态机：MERGING（双写目标+旧桶）→ MERGE_DRAIN（仅目标桶）→ ACTIVE。
func (s *Splitter) MergeEq(ctx context.Context, table, idxID, encVal string, slot uint16) error {
	key := bucketmap.Key(table, idxID, slot)
	for attempt := 0; attempt < maxStepRetries; attempt++ {
		sh, err := s.bm.LoadFresh(ctx, table, idxID, slot)
		if err != nil {
			return err
		}
		e := sh.Eq[encVal]
		if e == nil || len(e.Buckets) <= 1 {
			return nil // 单桶无需合并
		}
		if e.Split == nil {
			n := len(e.Buckets) / 2
			parents := make([]int, n)
			for i := range parents {
				parents[i] = sh.Next + i
			}
			if _, err := s.bm.CAS(ctx, key, sh.Version, "next", sh.Next+n); err != nil {
				continue
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID, slot)
			if err != nil {
				return err
			}
			entry := &bucketmap.EqEntry{
				Buckets: e.Buckets,
				Split:   &bucketmap.SplitInfo{State: bucketmap.Merging, Parents: parents, Children: e.Buckets},
			}
			if _, err := s.bm.CAS(ctx, key, sh2.Version, "e:"+encVal, entry); err != nil {
				continue
			}
			e = entry
		}

		if e.Split.State == bucketmap.Merging {
			// 搬迁：每个旧桶 → 合并目标桶（XXHash 取模）
			sm, _ := s.reg.Get("split_migrate")
			parentKeys := make([]string, 0, len(e.Split.Parents))
			for _, pIdx := range e.Split.Parents {
				parentKeys = append(parentKeys, keycodec.EqBucketKey(table, idxID, encVal, slot, pIdx))
			}
			for _, cIdx := range e.Split.Children {
				childKey := keycodec.EqBucketKey(table, idxID, encVal, slot, cIdx)
				for {
					res, err := s.cli.Do(ctx, "ZRANGE", childKey, 0, s.batch-1)
					if err != nil {
						return err
					}
					members := asStrings(res)
					if len(members) == 0 {
						break
					}
					if err := s.migrateBatch(ctx, sm, table, slot, childKey, parentKeys, members, func(m string) string {
						return keycodec.EqBucketKey(table, idxID, encVal, slot, e.Split.Parents[subOf(m, len(e.Split.Parents))])
					}); err != nil {
						return err
					}
					if len(members) < s.batch {
						break
					}
				}
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID, slot)
			if err != nil {
				return err
			}
			e2 := sh.Eq[encVal]
			if e2 == nil || e2.Split == nil {
				continue
			}
			e2.Split.State = bucketmap.MergeDrain
			if _, err := s.bm.CAS(ctx, key, sh2.Version, "e:"+encVal, e2); err != nil {
				continue
			}
			e = e2
		}

		if e.Split.State == bucketmap.MergeDrain {
			for _, cIdx := range e.Split.Children {
				if _, err := s.cli.Do(ctx, "UNLINK", keycodec.EqBucketKey(table, idxID, encVal, slot, cIdx)); err != nil {
					return err
				}
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID, slot)
			if err != nil {
				return err
			}
			final := &bucketmap.EqEntry{Buckets: e.Split.Parents}
			if _, err := s.bm.CAS(ctx, key, sh2.Version, "e:"+encVal, final); err != nil {
				continue
			}
			return nil
		}
	}
	return fmt.Errorf("%w: MergeEq %s/%s/%s slot %d", kidb.ErrStaleMetadata, table, idxID, encVal, slot)
}

// MergeRange 范围桶合并：相邻两桶 [lo,mid)+[mid,hi) 并为 [lo,hi)。
func (s *Splitter) MergeRange(ctx context.Context, table, idxID string, slot uint16, leftIdx int) error {
	key := bucketmap.Key(table, idxID, slot)
	for attempt := 0; attempt < maxStepRetries; attempt++ {
		sh, err := s.bm.LoadFresh(ctx, table, idxID, slot)
		if err != nil {
			return err
		}
		li, ri := -1, -1
		for i, rb := range sh.Ranges {
			if rb.Idx == leftIdx {
				li = i
			}
		}
		if li < 0 {
			return fmt.Errorf("controller: bucket %d not found", leftIdx)
		}
		// 找右邻（Lo == left.Hi）
		for i, rb := range sh.Ranges {
			if rb.Lo == sh.Ranges[li].Hi {
				ri = i
				break
			}
		}
		if ri < 0 {
			return fmt.Errorf("controller: no right sibling for bucket %d", leftIdx)
		}
		left, right := sh.Ranges[li], sh.Ranges[ri]

		if left.State == bucketmap.Active {
			merged := bucketmap.RangeBucket{Idx: sh.Next, Lo: left.Lo, Hi: right.Hi, State: bucketmap.Active}
			if _, err := s.bm.CAS(ctx, key, sh.Version, "next", sh.Next+1); err != nil {
				continue
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID, slot)
			if err != nil {
				return err
			}
			// 双桶同置 MERGING，Children[0]=合并目标
			for i := range sh2.Ranges {
				if sh2.Ranges[i].Idx == left.Idx || sh2.Ranges[i].Idx == right.Idx {
					sh2.Ranges[i].State = bucketmap.Merging
					sh2.Ranges[i].Children = []bucketmap.RangeBucket{merged, merged}
				}
			}
			if _, err := s.bm.CAS(ctx, key, sh2.Version, "r", sh2.Ranges); err != nil {
				continue
			}
			left.Children = []bucketmap.RangeBucket{merged, merged}
			right.Children = left.Children
			left.State, right.State = bucketmap.Merging, bucketmap.Merging
		}

		merged := left.Children[0]
		if left.State == bucketmap.Merging {
			// 两桶全部搬入合并目标
			for _, rb := range []bucketmap.RangeBucket{left, right} {
				if err := s.migrateRangeTo(ctx, table, idxID, slot, rb.Idx, merged); err != nil {
					return err
				}
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID, slot)
			if err != nil {
				return err
			}
			for i := range sh2.Ranges {
				if sh2.Ranges[i].Idx == left.Idx || sh2.Ranges[i].Idx == right.Idx {
					sh2.Ranges[i].State = bucketmap.MergeDrain
				}
			}
			if _, err := s.bm.CAS(ctx, key, sh2.Version, "r", sh2.Ranges); err != nil {
				continue
			}
			left.State, right.State = bucketmap.MergeDrain, bucketmap.MergeDrain
		}

		if left.State == bucketmap.MergeDrain {
			for _, rb := range []bucketmap.RangeBucket{left, right} {
				if _, err := s.cli.Do(ctx, "UNLINK", keycodec.RangeBucketKey(table, idxID, slot, rb.Idx)); err != nil {
					return err
				}
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID, slot)
			if err != nil {
				return err
			}
			var nr []bucketmap.RangeBucket
			for _, x := range sh2.Ranges {
				if x.Idx == left.Idx {
					nr = append(nr, merged) // 左位替换为合并桶
				} else if x.Idx == right.Idx {
					continue // 右位移除
				} else {
					nr = append(nr, x)
				}
			}
			if _, err := s.bm.CAS(ctx, key, sh2.Version, "r", nr); err != nil {
				continue
			}
			return nil
		}
	}
	return fmt.Errorf("%w: MergeRange %s/%s slot %d", kidb.ErrStaleMetadata, table, idxID, slot)
}

// migrateRangeTo 把某范围桶全部成员搬入目标桶（保 score）。
func (s *Splitter) migrateRangeTo(ctx context.Context, table, idxID string, slot uint16, fromIdx int, to bucketmap.RangeBucket) error {
	sm, _ := s.reg.Get("split_migrate")
	fromKey := keycodec.RangeBucketKey(table, idxID, slot, fromIdx)
	toKey := keycodec.RangeBucketKey(table, idxID, slot, to.Idx)
	for {
		res, err := s.cli.Do(ctx, "ZRANGE", fromKey, 0, s.batch-1, "WITHSCORES")
		if err != nil {
			return err
		}
		arr := asAnySlice(res)
		if len(arr) == 0 {
			return nil
		}
		var members []string
		targets := map[string]string{}
		scores := map[string]string{}
		for i := 0; i+1 < len(arr); i += 2 {
			m := fmt.Sprint(arr[i])
			members = append(members, m)
			scores[m] = fmt.Sprint(arr[i+1])
			targets[m] = toKey
		}
		if err := s.migrateBatchScored(ctx, sm, table, fromKey, []string{toKey}, members, targets, scores); err != nil {
			return err
		}
		if len(members) < s.batch {
			return nil
		}
	}
}
