package engine

import (
	"fmt"
	"math"

	"github.com/dolthub/go-mysql-server/sql"

	"kidb"
	"kidb/exec"
	"kidb/meta"
	"kidb/rowcodec"
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
		return nil, fmt.Errorf("%w: 非 MySQL 范围集合 %T", kidb.ErrUnsupported, lookup.Ranges)
	}

	// 主键路径：点查直取（HGETALL），范围退化为登记册遍历 + 谓词校验
	if idxID == "PRIMARY" {
		return t.translatePKLookup(ranges.ToRanges())
	}

	idx := t.def.Index(idxID)
	if idx == nil {
		return nil, fmt.Errorf("%w: 索引 %q 不存在于表 %q", kidb.ErrStaleMetadata, idxID, t.def.Name)
	}
	col := idx.Columns[0]

	switch idx.Kind {
	case meta.IndexEq, meta.IndexUnique:
		// 等值索引：每个 range 必须是点；非点范围退化全表遍历 + 字符串范围校验
		values := make([]string, 0, len(ranges))
		for _, r := range ranges {
			v, point, err := t.pointValue(r, col)
			if err != nil {
				return nil, err
			}
			if !point {
				return t.fullScanFallback(ranges.ToRanges(), col)
			}
			values = append(values, v)
		}
		return &exec.Request{
			Table: t.def, Kind: exec.EqLookup, Index: idx, Values: values,
			Pred: &exec.Predicate{Column: col, Eq: values},
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
		return &exec.Request{
			Table: t.def, Kind: exec.RangeLookup, Index: idx, Ranges: bounds,
			Pred: &exec.Predicate{Column: col, Ranges: bounds},
		}, nil
	}
	return nil, fmt.Errorf("%w: 未知索引形态 %v", kidb.ErrUnsupported, idx.Kind)
}

// translatePKLookup 主键：全部点 → PointGet；含非点范围 → 全扫 + 校验。
func (t *Table) translatePKLookup(ranges []sql.Range) (*exec.Request, error) {
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
	return &exec.Request{Table: t.def, Kind: exec.PointGet, Pks: pks}, nil
}

// fullScanFallback 非点范围兜底：exp 登记册遍历 + 谓词校验（始终正确）。
// 字符串列按字典序范围校验，数值列按 score 范围校验（docs/04 §4.1 兜底通道）。
func (t *Table) fullScanFallback(ranges []sql.Range, col string) (*exec.Request, error) {
	c, ok := t.def.Column(col)
	if !ok {
		return nil, fmt.Errorf("engine: 列 %q 不存在", col)
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
		return "", false, fmt.Errorf("%w: 非单列范围 %T", kidb.ErrUnsupported, r)
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
		return exec.RangeBound{}, fmt.Errorf("%w: 非单列范围 %T", kidb.ErrUnsupported, r)
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
		return exec.StrRange{}, fmt.Errorf("%w: 非单列范围 %T", kidb.ErrUnsupported, r)
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
		return "", fmt.Errorf("engine: 列 %q 不存在", col)
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
