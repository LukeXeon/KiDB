package engine

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"kidb"
	"kidb/exec"
	"kidb/i18n"
	"kidb/meta"
	"kidb/rowcodec"
	"kidb/utils"
)

// translateLookup 把 gms IndexLookup 翻译为 KiDB 物理计划（docs/04 §4.1 翻译表）。
//
// gms 侧范围语义（range_mysql.go/range_cut.go）：单列索引 → 每个 MySQLRange 恰含一个
// MySQLRangeColumnExpr；下界 Below{k}=闭、Above{k}=开、BelowAll=−∞；
// 上界 Above{k}=闭、Below{k}=开、AboveAll=+∞（gms 命名与直觉相反，以源码为准）。
func (t *Table) translateLookup(lookup sql.IndexLookup) (*exec.Request, error) {
	idxID := lookup.Index.ID()
	ranges, ok := lookup.Ranges.(sql.MySQLRangeCollection)
	if !ok {
		return nil, fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("err.range_set_type", lookup.Ranges))
	}

	// 主键路径：点查直取（HGETALL），范围退化为登记册遍历 + 谓词校验
	if idxID == "PRIMARY" {
		return t.translatePKLookup(ranges.ToRanges(), lookup.IsReverse)
	}

	idx := t.def.Index(idxID)
	if idx == nil {
		return nil, fmt.Errorf("%w: %s", kidb.ErrStaleMetadata, i18n.T("err.index_stale", idxID, t.def.Name))
	}
	col := idx.Columns[0]

	switch idx.Kind {
	case meta.IndexEq, meta.IndexUnique:
		// 等值索引：每个 range 必须是点；非点范围先看前缀形态（字典序副本），
		// 否则退化全表遍历 + 字符串范围校验
		values := make([]string, 0, len(ranges))
		for _, r := range ranges {
			v, point, err := t.pointValue(r, col)
			if err != nil {
				return nil, err
			}
			if !point {
				// 前缀区间形态 [p, p+\xff)（LookupForExpressions 自产）→
				// 字典序副本 ZRANGEBYLEX 归并路径（docs/04 §4.5）
				if idx.PrefixCopy {
					if lo, hi, ok := prefixRangeBounds(r); ok {
						return &exec.Request{
							Table: t.def, Kind: exec.PrefixLookup, Index: idx,
							LexLo: lo, LexHi: hi,
							Pred:       &exec.Predicate{Column: col, LikePrefix: lo},
							Projection: t.projectionForExec(),
						}, nil
					}
				}
				return t.fullScanFallback(ranges.ToRanges(), col)
			}
			values = append(values, v)
		}
		// 去重 + 按列值升序：gms 在 ORDER BY 索引列 ASC 时删 Sort 不咨询接口
		// （replace_sort.go Case A），点集产出序必须即索引列序
		values = t.dedupSortValues(col, values, lookup.IsReverse)
		return &exec.Request{
			Table: t.def, Kind: exec.EqLookup, Index: idx, Values: values,
			Pred:       &exec.Predicate{Column: col, Eq: values},
			Projection: t.projectionForExec(),
			Covering:   t.coveringOK(idx, col),
		}, nil

	case meta.IndexRange:
		bounds := make([]exec.RangeBound, 0, len(ranges))
		for _, r := range ranges {
			rb, err := t.rangeBound(r, col)
			if err != nil {
				return nil, err
			}
			bounds = append(bounds, rb)
		}
		// 区间排序：归并器逐区间拼接产出（topk.go），非重叠区间按 Lo 升序
		// （DESC 按 Hi 降序）拼接即全局有序
		sortRangeBounds(bounds, lookup.IsReverse)
		return &exec.Request{
			Table: t.def, Kind: exec.RangeLookup, Index: idx, Ranges: bounds,
			Pred:       &exec.Predicate{Column: col, Ranges: bounds},
			Desc:       lookup.IsReverse,
			Projection: t.projectionForExec(),
			Covering:   t.coveringOK(idx, col),
		}, nil
	}
	return nil, fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("err.unknown_index_kind", idx.Kind))
}

// coveringOK 覆盖索引命中判定（docs/03 §3.5）：投影 ∪ 谓词列 ⊆
// {索引列} ∪ 覆盖列 ∪ {pk} 时读路径跳过回表（member 自带覆盖列，
// 活性经 exp 登记册 ZSCORE 校验——同步索引 member 与行原子一致）。
// 异步索引不允许 covering（DDL 校验），此处无需再查。
func (t *Table) coveringOK(idx *meta.IndexDef, predCol string) bool {
	if len(idx.Covering) == 0 {
		return false
	}
	covered := utils.NewSet[string]()
	for _, c := range idx.Covering {
		covered.Add(strings.ToLower(c))
	}
	covered.Add(strings.ToLower(idx.Columns[0]))
	covered.Add(strings.ToLower(t.def.PK))
	need := func(c string) bool {
		return c != "" && !covered.Has(strings.ToLower(c))
	}
	if need(predCol) {
		return false
	}
	if !t.projected {
		// 未投影 = 全列（SELECT *）：含 _ttl 伪列（member 无 TTL 信息）→ 不覆盖
		return false
	}
	for _, c := range t.proj {
		if need(c) {
			return false
		}
	}
	return true
}

// translatePKLookup 主键：全部点 → PointGet；含非点范围 → 全扫 + 校验。
// 点集按 pk 值排序（gms 对 PRIMARY 点查同样删 Sort——见等值分支注记）。
func (t *Table) translatePKLookup(ranges []sql.Range, isReverse bool) (*exec.Request, error) {
	var pks []string
	for _, r := range ranges {
		v, point, err := t.pointValue(r, t.def.PK)
		if err != nil {
			return nil, err
		}
		if !point {
			return t.fullScanFallback(ranges, t.def.PK)
		}
		pks = append(pks, v)
	}
	pks = t.dedupSortValues(t.def.PK, pks, isReverse)
	return &exec.Request{Table: t.def, Kind: exec.PointGet, Pks: pks, Projection: t.projectionForExec()}, nil
}

// dedupSortValues 去重并按列类型解码值排序（编码串的字典序 ≠ 数值序，如 "10"<"2"）。
func (t *Table) dedupSortValues(col string, values []string, desc bool) []string {
	c, ok := t.def.Column(col)
	if !ok {
		return values
	}
	seen := make(utils.Set[string], len(values))
	out := values[:0]
	for _, v := range values {
		if seen.Has(v) {
			continue
		}
		seen.Add(v)
		out = append(out, v)
	}
	type pair struct {
		enc string
		dec any
	}
	pairs := make([]pair, 0, len(out))
	for _, v := range out {
		d, err := rowcodec.Decode(c.Type, v)
		if err != nil {
			return out // 解码失败保持原序（校验侧拦截，docs/04 §4.3）
		}
		pairs = append(pairs, pair{v, d})
	}
	compare := func(x, y pair) int {
		a, b := x.dec, y.dec
		switch av := a.(type) {
		case int64:
			return cmp.Compare(av, b.(int64))
		case float64:
			return cmp.Compare(av, b.(float64))
		case time.Time:
			return av.Compare(b.(time.Time))
		case string:
			return cmp.Compare(av, b.(string))
		case []byte:
			return cmp.Compare(string(av), string(b.([]byte)))
		}
		return cmp.Compare(x.enc, y.enc)
	}
	if desc {
		slices.SortStableFunc(pairs, func(x, y pair) int { return compare(y, x) })
	} else {
		slices.SortStableFunc(pairs, compare)
	}
	for i := range pairs {
		out[i] = pairs[i].enc
	}
	return out
}

// sortRangeBounds 区间排序（归并逐区间拼接的前提）。
func sortRangeBounds(bounds []exec.RangeBound, desc bool) {
	if desc {
		slices.SortStableFunc(bounds, func(x, y exec.RangeBound) int { return cmp.Compare(y.Hi, x.Hi) })
		return
	}
	slices.SortStableFunc(bounds, func(x, y exec.RangeBound) int { return cmp.Compare(x.Lo, y.Lo) })
}

// fullScanFallback 非点范围兜底：exp 登记册遍历 + 谓词校验（始终正确）。
// 字符串列按字典序范围校验，数值列按 score 范围校验（docs/04 §4.1 兜底通道）。
func (t *Table) fullScanFallback(ranges []sql.Range, col string) (*exec.Request, error) {
	c, ok := t.def.Column(col)
	if !ok {
		return nil, fmt.Errorf("%s", i18n.T("err.column_missing", col))
	}
	pred := &exec.Predicate{Column: col}
	if c.Type == meta.ColString || c.Type == meta.ColBytes {
		var srs []exec.StrRange
		for _, r := range ranges {
			sr, err := t.strRange(r, col)
			if err != nil {
				return nil, err
			}
			srs = append(srs, sr)
		}
		pred.Str = srs
	} else {
		var rbs []exec.RangeBound
		for _, r := range ranges {
			rb, err := t.rangeBound(r, col)
			if err != nil {
				return nil, err
			}
			rbs = append(rbs, rb)
		}
		pred.Ranges = rbs
	}
	return &exec.Request{Table: t.def, Kind: exec.FullScan, Pred: pred}, nil
}

// pointValue 提取点查值（点 = 下界闭、上界闭、两界相等）。返回编码后的存储形态。
func (t *Table) pointValue(r sql.Range, col string) (string, bool, error) {
	mr, ok := r.(sql.MySQLRange)
	if !ok || len(mr) != 1 {
		return "", false, fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("err.non_single_range", r))
	}
	ce := mr[0]
	lo, lok := belowKey(ce.LowerBound)
	hi, hok := aboveKey(ce.UpperBound)
	if !lok || !hok {
		return "", false, nil
	}
	encLo, err := t.encodeCol(col, lo)
	if err != nil {
		return "", false, err
	}
	encHi, err := t.encodeCol(col, hi)
	if err != nil {
		return "", false, err
	}
	if encLo != encHi {
		return "", false, nil
	}
	return encLo, true, nil
}

// rangeBound 翻译 score 区间。
func (t *Table) rangeBound(r sql.Range, col string) (exec.RangeBound, error) {
	mr, ok := r.(sql.MySQLRange)
	if !ok || len(mr) != 1 {
		return exec.RangeBound{}, fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("err.non_single_range", r))
	}
	ce := mr[0]
	var rb exec.RangeBound
	switch lb := ce.LowerBound.(type) {
	case sql.Below: // 闭下界 [lo
		f, err := t.scoreOf(col, lb.Key)
		if err != nil {
			return rb, err
		}
		rb.Lo = f
	case sql.Above: // 开下界 (lo
		f, err := t.scoreOf(col, lb.Key)
		if err != nil {
			return rb, err
		}
		rb.Lo, rb.LoOpen = f, true
	default: // BelowAll / BelowNull → −∞
		rb.Lo = math.Inf(-1)
	}
	switch ub := ce.UpperBound.(type) {
	case sql.Above: // 闭上界 hi]
		f, err := t.scoreOf(col, ub.Key)
		if err != nil {
			return rb, err
		}
		rb.Hi = f
	case sql.Below: // 开上界 hi)
		f, err := t.scoreOf(col, ub.Key)
		if err != nil {
			return rb, err
		}
		rb.Hi, rb.HiOpen = f, true
	default: // AboveAll / AboveNull → +∞
		rb.Hi = math.Inf(1)
	}
	return rb, nil
}

// strRange 翻译字符串字典序区间（回表校验用）。
func (t *Table) strRange(r sql.Range, col string) (exec.StrRange, error) {
	mr, ok := r.(sql.MySQLRange)
	if !ok || len(mr) != 1 {
		return exec.StrRange{}, fmt.Errorf("%w: %s", kidb.ErrUnsupported, i18n.T("err.non_single_range", r))
	}
	ce := mr[0]
	var sr exec.StrRange
	switch lb := ce.LowerBound.(type) {
	case sql.Below:
		v, err := t.encodeCol(col, lb.Key)
		if err != nil {
			return sr, err
		}
		sr.Lo = v
	case sql.Above:
		v, err := t.encodeCol(col, lb.Key)
		if err != nil {
			return sr, err
		}
		sr.Lo, sr.LoOpen = v, true
	default:
		sr.LoInf = true
	}
	switch ub := ce.UpperBound.(type) {
	case sql.Above:
		v, err := t.encodeCol(col, ub.Key)
		if err != nil {
			return sr, err
		}
		sr.Hi = v
	case sql.Below:
		v, err := t.encodeCol(col, ub.Key)
		if err != nil {
			return sr, err
		}
		sr.Hi, sr.HiOpen = v, true
	default:
		sr.HiInf = true
	}
	return sr, nil
}

// encodeCol 按列类型编码值。
func (t *Table) encodeCol(col string, v any) (string, error) {
	c, ok := t.def.Column(col)
	if !ok {
		return "", fmt.Errorf("%s", i18n.T("err.column_missing", col))
	}
	return rowcodec.Encode(c.Type, v)
}

// scoreOf 编码后转 score（范围索引用）。
func (t *Table) scoreOf(col string, v any) (float64, error) {
	enc, err := t.encodeCol(col, v)
	if err != nil {
		return 0, err
	}
	c, _ := t.def.Column(col)
	return rowcodec.ScoreOf(c.Type, enc)
}

// belowKey 取闭下界键值（gms：Below 作下界 = 包含）。
func belowKey(c sql.MySQLRangeCut) (any, bool) {
	if b, ok := c.(sql.Below); ok {
		return b.Key, true
	}
	return nil, false
}

// aboveKey 取闭上界键值（gms：Above 作上界 = 包含）。
func aboveKey(c sql.MySQLRangeCut) (any, bool) {
	if a, ok := c.(sql.Above); ok {
		return a.Key, true
	}
	return nil, false
}
