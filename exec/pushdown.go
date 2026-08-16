package exec

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"kidb/keycodec"
	"kidb/rowcodec"
	"kidb/utils"
)

// pushdown.go：服务端谓词下推（docs/04 §4.2）。
// 两轮协议：轮1 客户端已从桶取候选 pk；轮2 按 slot 分组执行 pushdown_filter.lua，
// 服务端完成"回表 + 谓词过滤"，网络只传命中行。

// pushdownable 报告谓词是否属于下推白名单（单列等值集合 / 单列双侧有限区间）。
func (p *Predicate) pushdownable() bool {
	if p == nil || p.Column == "" {
		return false
	}
	if p.Eq != nil {
		return true
	}
	if len(p.Ranges) == 1 {
		r := p.Ranges[0]
		return !math.IsInf(r.Lo, 0) && !math.IsInf(r.Hi, 0) // Lua tonumber 不认 inf
	}
	return false
}

// pushdownArgs 编译 ARGV。
func (p *Predicate) pushdownArgs() []any {
	if p.Eq != nil {
		argv := []any{p.Column, "eq", strconv.Itoa(len(p.Eq))}
		for _, v := range p.Eq {
			argv = append(argv, v)
		}
		return argv
	}
	r := p.Ranges[0]
	return []any{
		p.Column, "range",
		strconv.FormatFloat(r.Lo, 'g', -1, 64), strconv.FormatFloat(r.Hi, 'g', -1, 64),
		bool01(r.LoOpen), bool01(r.HiOpen),
	}
}

// fetchRowsPushdown 服务端过滤路径（替代客户端回表校验的等价实现，P4 对拍对象）。
// ctx 通过 RowStream 传入（context 取消贯穿，契约 R6）。
func (s *RowStream) fetchRowsPushdown(ctx context.Context, pks []string) error {
	pd, ok := s.exec.reg.Get("pushdown_filter")
	if !ok {
		return fmt.Errorf("exec: pushdown_filter.lua not registered")
	}
	t := s.req.Table

	// 按 slot 分组（集群模式 Lua 只访问同 slot key，契约 R3）
	groups := map[uint16][]string{}
	for _, pk := range pks {
		rk := keycodec.RowKey(t.Name, pk)
		slot := keycodec.Slot(rk)
		groups[slot] = append(groups[slot], rk)
	}

	for _, keys := range groups {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		out, err := s.exec.cli.Eval(s.ctx, pd, keys, s.req.Pred.pushdownArgs()...)
		if err != nil {
			return fmt.Errorf("exec: pushdown eval: %w", err)
		}
		rows, err := s.parsePushdownReply(out)
		if err != nil {
			return err
		}
		s.rows = append(s.rows, rows...)
	}
	return nil
}

// parsePushdownReply 解析扁平返回 [pk, fieldCount, (f, v)×n, ...]。
func (s *RowStream) parsePushdownReply(out any) ([][]any, error) {
	arr := utils.AnySlice(out)
	var rows [][]any
	i := 0
	for i < len(arr) {
		if i+2 > len(arr) {
			return nil, fmt.Errorf("exec: pushdown reply truncated at %d", i)
		}
		pk := fmt.Sprint(arr[i])
		nf, err := strconv.Atoi(fmt.Sprint(arr[i+1]))
		if err != nil {
			return nil, fmt.Errorf("exec: pushdown reply bad field count %v", arr[i+1])
		}
		i += 2
		if i+2*nf > len(arr) {
			return nil, fmt.Errorf("exec: pushdown reply truncated fields")
		}
		raw := make(map[string]string, nf)
		for j := 0; j < nf; j++ {
			raw[fmt.Sprint(arr[i+2*j])] = fmt.Sprint(arr[i+2*j+1])
		}
		i += 2 * nf
		rows = append(rows, rowcodec.DecodeRowCols(s.req.Table, pk, raw, s.req.Projection))
	}
	return rows, nil
}

func bool01(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
