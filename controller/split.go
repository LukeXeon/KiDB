// Package controller 是自治控制循环（docs/08）：锁选举（含 watchdog 闭环）、
// 桶分裂/合并状态机、L4 热桶副本生命周期、DDL 作业执行、漂移对账。
//
// split.go：桶分裂/合并（docs/08 §8.2/§8.3，v7.0 异 slot 形态）。
// v7.0：子桶与父桶异 slot（tag 编入子桶号摊开）——搬迁不再是单 slot 原子 Lua，
// 改由"版本戳精确 member 幂等 + DRAINING 排干"保证（docs/05 §5.1 同族论证）：
// 客户端批搬迁（ZRANGE → EXISTS 活性过滤 → ZADD 子桶 → ZREM 父桶，全部精确
// member 幂等），排干循环收容快照过期的迟到写（父桶不排空不得 DELETED）。
// split_migrate.lua 随之退役（v7.0 删除资产）。
package controller

import (
	"context"
	"fmt"
	"strconv"

	"kidb"
	"kidb/bucketmap"
	"kidb/keycodec"
	"kidb/kv"
	"kidb/metrics"
	"kidb/rowcodec"
	"kidb/script"
	"kidb/utils"
)

// maxStepRetries 单步 CAS 冲突重试上限（docs/08 §8.3）。
const maxStepRetries = 8

// Splitter 执行桶分裂/合并状态机步进（Controller 驱动）。
type Splitter struct {
	cli   kv.Client
	reg   *script.Registry
	bm    *bucketmap.Store
	m     *metrics.Metrics // 指标（nil = no-op）
	batch int
}

// NewSplitter 构造（batch 默认 500，docs/08 §8.3）。
func NewSplitter(cli kv.Client, reg *script.Registry, bm *bucketmap.Store) *Splitter {
	return &Splitter{cli: cli, reg: reg, bm: bm, batch: 500}
}

// SetMetrics 接入指标。
func (s *Splitter) SetMetrics(m *metrics.Metrics) { s.m = m }

// SplitEq 等值热值分裂：当前 n 子桶 → 2n 子桶（写/删按 xxhash64(pk)%2n 选桶）。
// covering 来自索引定义（member 解析需要，schema 感知）。
func (s *Splitter) SplitEq(ctx context.Context, table, idxID, encVal string, covering bool) error {
	key := keycodec.BucketMapKey(table, idxID)

	for attempt := 0; attempt < maxStepRetries; attempt++ {
		sh, err := s.bm.LoadFresh(ctx, table, idxID)
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
			sh2, err := s.bm.LoadFresh(ctx, table, idxID)
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

		// 第 2 步：分批搬迁 + 排干（父桶取首页直到空——搬迁即删，首页恒为剩余头部；
		// 迟到写由循环收容，docs/08 §8.3）
		if e.Split.State == bucketmap.Splitting {
			if err := s.migrateEq(ctx, table, idxID, encVal, e.Split, covering); err != nil {
				return err
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID)
			if err != nil {
				return err
			}
			e2 := sh2.Eq[encVal] // 必须读新鲜文档（review 实证曾误用旧 sh）
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
				if _, err := s.cli.Do(ctx, "UNLINK", keycodec.EqBucketKeyEsc(table, idxID, encVal, pIdx)); err != nil {
					return err
				}
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID)
			if err != nil {
				return err
			}
			final := &bucketmap.EqEntry{Buckets: e.Split.Children}
			if _, err := s.bm.CAS(ctx, key, sh2.Version, "e:"+encVal, final); err != nil {
				continue
			}
			if s.m != nil {
				s.m.Splits.Inc()
			}
			return s.bm.RegisterHot(ctx, table, idxID, encVal)
		}
	}
	return fmt.Errorf("%w: SplitEq %s/%s/%s", kidb.ErrStaleMetadata, table, idxID, encVal)
}

// memberTarget 一条待搬迁成员（精确 member + 目标桶 + score）。
type memberTarget struct {
	member, childKey, score string
}

// migrateBatch 一批成员的搬迁（客户端幂等形态，v7.0）：
// EXISTS 行key 活性过滤（死成员顺路清理）→ 活成员 ZADD 目标桶（精确 member
// 幂等）→ 全部 ZREM 父桶（同批精确 member）。批间崩溃：member 父子双存，
// 读侧 seen 去重；重放幂等（docs/08 §8.3）。
func (s *Splitter) migrateBatch(ctx context.Context, table, parentKey string, batch []memberTarget, covering bool) error {
	// 1. 活性判定（同 pipeline）
	ecmds := make([]kv.Cmd, 0, len(batch))
	for _, mt := range batch {
		pk := rowcodec.MemberPK(mt.member, covering)
		ecmds = append(ecmds, kv.Cmd{Name: "EXISTS", Args: []any{keycodec.RowKey(table, pk)}})
	}
	alive, err := s.cli.Pipeline(ctx, ecmds)
	if err != nil {
		return err
	}
	// 2. ZADD 活成员 + ZREM 全部
	cmds := make([]kv.Cmd, 0, 2*len(batch))
	for i, mt := range batch {
		if fmt.Sprint(alive[i]) == "1" {
			cmds = append(cmds, kv.Cmd{Name: "ZADD", Args: []any{mt.childKey, mt.score, mt.member}})
		}
		cmds = append(cmds, kv.Cmd{Name: "ZREM", Args: []any{parentKey, mt.member}})
	}
	_, err = s.cli.Pipeline(ctx, cmds)
	return err
}

// migrateEq 搬迁：每父桶循环取首页 → 按 keycodec.EqSubFor(pk, len(children))
// 选子桶（与写路径 WriteTargetsEq 同一函数——放置一致性纪律）→ 客户端批迁移。
func (s *Splitter) migrateEq(ctx context.Context, table, idxID, encVal string, sp *bucketmap.SplitInfo, covering bool) error {
	for _, pIdx := range sp.Parents {
		parentKey := keycodec.EqBucketKeyEsc(table, idxID, encVal, pIdx)
		for {
			res, err := s.cli.Do(ctx, "ZRANGE", parentKey, 0, s.batch-1)
			if err != nil {
				return err
			}
			members := utils.Strings(res)
			if len(members) == 0 {
				break
			}
			batch := make([]memberTarget, 0, len(members))
			for _, m := range members {
				pk := rowcodec.MemberPK(m, covering)
				childIdx := sp.Children[keycodec.EqSubFor(pk, len(sp.Children))]
				batch = append(batch, memberTarget{m, keycodec.EqBucketKeyEsc(table, idxID, encVal, childIdx), "0"})
			}
			if err := s.migrateBatch(ctx, table, parentKey, batch, covering); err != nil {
				return err
			}
			if len(members) < s.batch {
				break
			}
		}
	}
	return nil
}

// SplitRange 范围桶分裂：指定桶按采样中位数裂为 [lo,mid) [mid,hi)。
func (s *Splitter) SplitRange(ctx context.Context, table, idxID string, bucketIdx int, covering bool) error {
	key := keycodec.BucketMapKey(table, idxID)

	for attempt := 0; attempt < maxStepRetries; attempt++ {
		sh, err := s.bm.LoadFresh(ctx, table, idxID)
		if err != nil {
			return err
		}
		rbPos := -1
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
			mid, err := s.sampleMedian(ctx, table, idxID, bucketIdx)
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
			sh2, err := s.bm.LoadFresh(ctx, table, idxID)
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
			if err := s.migrateRange(ctx, table, idxID, rb, covering); err != nil {
				return err
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID)
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
			if _, err := s.cli.Do(ctx, "UNLINK", keycodec.RangeBucketKey(table, idxID, rb.Idx)); err != nil {
				return err
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID)
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
	return fmt.Errorf("%w: SplitRange %s/%s", kidb.ErrStaleMetadata, table, idxID)
}

// sampleMedian 采样估算区间中位数（docs/08 §8.2）。
// 多分位采样（review 实证修正：首页 511 个是 score 最低的成员——分裂点系统性
// 偏低、大桶退化为多次裂变）：ZCARD 后按 4 分位点各取 1 个 score 取中位。
func (s *Splitter) sampleMedian(ctx context.Context, table, idxID string, bucketIdx int) (float64, error) {
	bk := keycodec.RangeBucketKey(table, idxID, bucketIdx)
	card, err := s.cli.Do(ctx, "ZCARD", bk)
	if err != nil {
		return 0, err
	}
	n, _ := strconv.ParseInt(fmt.Sprint(card), 10, 64)
	if n == 0 {
		return 0, fmt.Errorf("controller: range bucket empty, nothing to split")
	}
	var cmds []kv.Cmd
	for _, q := range []int64{1, 2, 3} { // 25%/50%/75% 分位点
		off := n * q / 4
		cmds = append(cmds, kv.Cmd{Name: "ZRANGE", Args: []any{bk, off, off, "WITHSCORES"}})
	}
	results, err := s.cli.Pipeline(ctx, cmds)
	if err != nil {
		return 0, err
	}
	var vals []float64
	for _, res := range results {
		arr := utils.AnySlice(res)
		if len(arr) >= 2 {
			if f, err := strconv.ParseFloat(fmt.Sprint(arr[1]), 64); err == nil {
				vals = append(vals, f)
			}
		}
	}
	if len(vals) == 0 {
		return 0, fmt.Errorf("controller: range bucket sample empty")
	}
	return vals[len(vals)/2], nil
}

// migrateRange 范围桶搬迁（按 score 选子桶区间，保 score）。
func (s *Splitter) migrateRange(ctx context.Context, table, idxID string, rb bucketmap.RangeBucket, covering bool) error {
	parentKey := keycodec.RangeBucketKey(table, idxID, rb.Idx)
	mid := boundOf(rb.Children[1].Lo)
	for {
		res, err := s.cli.Do(ctx, "ZRANGE", parentKey, 0, s.batch-1, "WITHSCORES")
		if err != nil {
			return err
		}
		arr := utils.AnySlice(res)
		if len(arr) == 0 {
			return nil
		}
		var batch []memberTarget
		for i := 0; i+1 < len(arr); i += 2 {
			m := fmt.Sprint(arr[i])
			sc := fmt.Sprint(arr[i+1])
			f, _ := strconv.ParseFloat(sc, 64)
			child := rb.Children[1]
			if f < mid {
				child = rb.Children[0]
			}
			batch = append(batch, memberTarget{m, keycodec.RangeBucketKey(table, idxID, child.Idx), sc})
		}
		if err := s.migrateBatch(ctx, table, parentKey, batch, covering); err != nil {
			return err
		}
		if len(arr)/2 < s.batch {
			return nil
		}
	}
}

func boundOf(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// ==== 合并（分裂的镜像协议，docs/08 §8.3 末段）====

// MergeEq 等值桶合并：当前 2n 子桶并为 n 个（持续低基数的回收路径）。
// 镜像状态机：MERGING（双写目标+旧桶）→ MERGE_DRAIN（仅目标桶）→ ACTIVE。
func (s *Splitter) MergeEq(ctx context.Context, table, idxID, encVal string, covering bool) error {
	key := keycodec.BucketMapKey(table, idxID)
	for attempt := 0; attempt < maxStepRetries; attempt++ {
		sh, err := s.bm.LoadFresh(ctx, table, idxID)
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
			sh2, err := s.bm.LoadFresh(ctx, table, idxID)
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
			// 搬迁：每个旧桶 → 合并目标桶（EqSubFor 同一函数选桶）
			for _, cIdx := range e.Split.Children {
				childKey := keycodec.EqBucketKeyEsc(table, idxID, encVal, cIdx)
				for {
					res, err := s.cli.Do(ctx, "ZRANGE", childKey, 0, s.batch-1)
					if err != nil {
						return err
					}
					members := utils.Strings(res)
					if len(members) == 0 {
						break
					}
					batch := make([]memberTarget, 0, len(members))
					for _, m := range members {
						pk := rowcodec.MemberPK(m, covering)
						tIdx := e.Split.Parents[keycodec.EqSubFor(pk, len(e.Split.Parents))]
						batch = append(batch, memberTarget{m, keycodec.EqBucketKeyEsc(table, idxID, encVal, tIdx), "0"})
					}
					if err := s.migrateBatch(ctx, table, childKey, batch, covering); err != nil {
						return err
					}
					if len(members) < s.batch {
						break
					}
				}
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID)
			if err != nil {
				return err
			}
			e2 := sh2.Eq[encVal] // 同上：新鲜文档
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
				if _, err := s.cli.Do(ctx, "UNLINK", keycodec.EqBucketKeyEsc(table, idxID, encVal, cIdx)); err != nil {
					return err
				}
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID)
			if err != nil {
				return err
			}
			final := &bucketmap.EqEntry{Buckets: e.Split.Parents}
			if _, err := s.bm.CAS(ctx, key, sh2.Version, "e:"+encVal, final); err != nil {
				continue
			}
			if s.m != nil {
				s.m.Merges.Inc()
			}
			return nil
		}
	}
	return fmt.Errorf("%w: MergeEq %s/%s/%s", kidb.ErrStaleMetadata, table, idxID, encVal)
}

// MergeRange 范围桶合并：相邻两桶 [lo,mid)+[mid,hi) 并为 [lo,hi)。
func (s *Splitter) MergeRange(ctx context.Context, table, idxID string, leftIdx int, covering bool) error {
	key := keycodec.BucketMapKey(table, idxID)
	for attempt := 0; attempt < maxStepRetries; attempt++ {
		sh, err := s.bm.LoadFresh(ctx, table, idxID)
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
			sh2, err := s.bm.LoadFresh(ctx, table, idxID)
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
				if err := s.migrateRangeTo(ctx, table, idxID, rb.Idx, merged, covering); err != nil {
					return err
				}
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID)
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
				if _, err := s.cli.Do(ctx, "UNLINK", keycodec.RangeBucketKey(table, idxID, rb.Idx)); err != nil {
					return err
				}
			}
			sh2, err := s.bm.LoadFresh(ctx, table, idxID)
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
			if s.m != nil {
				s.m.Merges.Inc()
			}
			return nil
		}
	}
	return fmt.Errorf("%w: MergeRange %s/%s", kidb.ErrStaleMetadata, table, idxID)
}

// migrateRangeTo 把某范围桶全部成员搬入目标桶（保 score）。
func (s *Splitter) migrateRangeTo(ctx context.Context, table, idxID string, fromIdx int, to bucketmap.RangeBucket, covering bool) error {
	fromKey := keycodec.RangeBucketKey(table, idxID, fromIdx)
	toKey := keycodec.RangeBucketKey(table, idxID, to.Idx)
	for {
		res, err := s.cli.Do(ctx, "ZRANGE", fromKey, 0, s.batch-1, "WITHSCORES")
		if err != nil {
			return err
		}
		arr := utils.AnySlice(res)
		if len(arr) == 0 {
			return nil
		}
		var batch []memberTarget
		for i := 0; i+1 < len(arr); i += 2 {
			m := fmt.Sprint(arr[i])
			batch = append(batch, memberTarget{m, toKey, fmt.Sprint(arr[i+1])})
		}
		if err := s.migrateBatch(ctx, table, fromKey, batch, covering); err != nil {
			return err
		}
		if len(arr)/2 < s.batch {
			return nil
		}
	}
}
