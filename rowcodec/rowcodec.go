// Package rowcodec 负责 gms 类型值 ↔ Redis 字符串编码的转换
// （docs/03 §3.4：标量字段 Hash 平铺零序列化；JSON 列归一化文本直存——
// 写入时解析归一（key 排序/数字 float64 归一），存储形态即可直读文本）。
package rowcodec

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tinylib/msgp/msgp"

	"kidb/i18n"
	"kidb/meta"
)

// Encode 把 SQL 层值编码为 Redis 字符串形态。
// 调用方应先用列类型 Convert 归一（engine 层负责），此处做宽松接受。
func Encode(ct meta.ColumnType, v any) (string, error) {
	if v == nil {
		return "", fmt.Errorf("rowcodec: nil value for %v", ct)
	}
	switch ct {
	case meta.ColInt:
		switch n := v.(type) {
		case int64:
			return strconv.FormatInt(n, 10), nil
		case int:
			return strconv.Itoa(n), nil
		case int32:
			return strconv.FormatInt(int64(n), 10), nil
		case uint64:
			return strconv.FormatUint(n, 10), nil
		case float64:
			return strconv.FormatInt(int64(n), 10), nil
		case string:
			return n, nil
		}
	case meta.ColFloat:
		switch n := v.(type) {
		case float64:
			return strconv.FormatFloat(n, 'g', -1, 64), nil
		case float32:
			return strconv.FormatFloat(float64(n), 'g', -1, 64), nil
		case int64:
			return strconv.FormatInt(n, 10), nil
		case string:
			return n, nil
		}
	case meta.ColString:
		if s, ok := v.(string); ok {
			return s, nil
		}
		return fmt.Sprint(v), nil
	case meta.ColBytes:
		switch b := v.(type) {
		case []byte:
			return string(b), nil
		case string:
			return b, nil
		}
	case meta.ColTimestamp:
		switch t := v.(type) {
		case time.Time:
			return strconv.FormatInt(t.Unix(), 10), nil
		case int64:
			return strconv.FormatInt(t, 10), nil
		case string:
			return t, nil
		}
	case meta.ColJSON:
		return EncodeJSON(v) // 归一化文本直存（docs/03 §3.4）
	}
	return "", fmt.Errorf("rowcodec: cannot encode %T into %v", v, ct)
}

// Decode 把 Redis 字符串还原为指定列类型的值。
func Decode(ct meta.ColumnType, s string) (any, error) {
	switch ct {
	case meta.ColInt:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("rowcodec: decode int %q: %w", s, err)
		}
		return n, nil
	case meta.ColFloat:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("rowcodec: decode float %q: %w", s, err)
		}
		return f, nil
	case meta.ColString:
		return s, nil
	case meta.ColBytes:
		return []byte(s), nil
	case meta.ColTimestamp:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("rowcodec: decode ts %q: %w", s, err)
		}
		return time.Unix(n, 0).UTC(), nil
	case meta.ColJSON:
		return DecodeJSON(s)
	}
	return nil, fmt.Errorf("rowcodec: unknown type %v", ct)
}

// ScoreOf 返回编码值对应的 ZSet score（范围索引用，docs/03 §3.4：
// int/float 直存 float64 有序无损，int64 越 2^53 禁止范围索引——DDL 校验）。
func ScoreOf(ct meta.ColumnType, encoded string) (float64, error) {
	switch ct {
	case meta.ColInt, meta.ColTimestamp, meta.ColFloat:
		return strconv.ParseFloat(encoded, 64)
	}
	return 0, fmt.Errorf("rowcodec: %s", i18n.T("codec.not_scoreable", ct))
}

// EncodeRow 把整行字段映射编码为存储形态（供 txguard.Fields）。
func EncodeRow(t *meta.TableDef, fields map[string]any) (map[string]string, error) {
	out := make(map[string]string, len(fields))
	for name, v := range fields {
		col, ok := t.Column(name)
		if !ok {
			return nil, fmt.Errorf("rowcodec: %s", i18n.T("codec.unknown_column", name))
		}
		enc, err := Encode(col.Type, v)
		if err != nil {
			return nil, err
		}
		out[col.Name] = enc
	}
	return out, nil
}

// DecodeRow 把存储形态解码为按 schema 列序的行（NULL = 字段缺失，docs/04 §4.3）。
// pk 由调用方从 key/member 侧注入（对齐 TiDB：handle 在 key 里而不在行值里）。
func DecodeRow(t *meta.TableDef, pk string, raw map[string]string) []any {
	row := make([]any, 0, len(t.Columns))
	for _, col := range t.Columns {
		if col.Name == t.PK {
			v, err := Decode(col.Type, pk)
			if err != nil {
				row = append(row, nil)
			} else {
				row = append(row, v)
			}
			continue
		}
		s, ok := raw[col.Name]
		if !ok {
			row = append(row, nil)
			continue
		}
		v, err := Decode(col.Type, s)
		if err != nil {
			row = append(row, nil) // 编码错误按 NULL 处理并由校验侧拦截
			continue
		}
		row = append(row, v)
	}
	// _ttl 伪列恒挂 schema 尾部（docs/07 §7.1；SELECT * 含它——gms 无隐藏列
	// 机制的诚实取舍）。raw 无该键 = 非 PTTL 感知路径（如编辑器预读）→ NULL。
	if v := raw[meta.TTLPseudoColumn]; v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			row = append(row, n)
		} else {
			row = append(row, nil)
		}
	} else {
		row = append(row, nil)
	}
	return row
}

// DecodeRowCols 按指定列序解码（投影下推，docs/04 §4.3：gms ProjectedTable
// 契约要求行与投影后的 Schema 同宽同序）。
// cols == nil = 全 schema 列序；空非 nil = 零宽行（COUNT 类只数行不读列）。
func DecodeRowCols(t *meta.TableDef, pk string, raw map[string]string, cols []string) []any {
	if cols == nil {
		return DecodeRow(t, pk, raw)
	}
	row := make([]any, 0, len(cols))
	for _, name := range cols {
		if strings.EqualFold(name, meta.TTLPseudoColumn) {
			// _ttl 伪列：exec 读路径把行剩余 TTL 秒写入 raw["_ttl"]（PTTL 自省）
			v := raw[meta.TTLPseudoColumn]
			n, err := strconv.ParseInt(v, 10, 64)
			if v == "" || err != nil {
				row = append(row, nil)
				continue
			}
			row = append(row, n)
			continue
		}
		col, ok := t.Column(name)
		if !ok {
			row = append(row, nil)
			continue
		}
		if col.Name == t.PK {
			v, err := Decode(col.Type, pk)
			if err != nil {
				row = append(row, nil)
			} else {
				row = append(row, v)
			}
			continue
		}
		s, ok := raw[col.Name]
		if !ok {
			row = append(row, nil)
			continue
		}
		v, err := Decode(col.Type, s)
		if err != nil {
			row = append(row, nil)
			continue
		}
		row = append(row, v)
	}
	return row
}

// ==== 桶 member 编码（docs/03 §3.4/§3.5，v7.0 版本戳）====
// v7.0：member 携带行版本戳（两段写的并发交错安全与幂等重试依据，docs/05 §5.1）。
// 无覆盖列：member = pk \x1f ver（pk 仍近零序列化；解析取末段 \x1f 后数字）。
// 有覆盖列：member = msgp 数组 [pk, ver, cover1, ...]（代码生成级格式，二进制安全）。
// 读取侧经索引定义得知是否有覆盖列（schema 感知，无格式猜测）。

// PlainMember 无覆盖列桶 member：`pk \x1f ver`。
func PlainMember(pk string, ver uint64) string {
	return pk + "\x1f" + strconv.FormatUint(ver, 10)
}

// LexMember 字典序副本 member：value + \x00 + pk + \x1f + ver
// （字典序按值再按 pk 唯一；ver 在 pk 段后，同 (值,pk) 的版本残留自然相邻，
// 读取侧按 pk 去重）。txguard 写路径与 JobRunner 回填共用（编码单点）。
func LexMember(value, pk string, ver uint64) string {
	return value + "\x00" + pk + "\x1f" + strconv.FormatUint(ver, 10)
}

// EncodeMember 生成桶 member（覆盖形态：msgp 数组 [pk, ver, cover1, ...]）。
func EncodeMember(pk string, ver uint64, covers []string) string {
	if len(covers) == 0 {
		return PlainMember(pk, ver)
	}
	b := msgp.AppendArrayHeader(nil, uint32(2+len(covers)))
	b = msgp.AppendString(b, pk)
	b = msgp.AppendString(b, strconv.FormatUint(ver, 10))
	for _, c := range covers {
		b = msgp.AppendString(b, c)
	}
	return string(b)
}

// MemberPK 提取 member 中的 pk。hasCovering 来自索引定义（schema 感知）。
// v7.0：无覆盖形态取末段 \x1f 前部（ver 为尾随十进制数字；解析失败按原样——
// 原始 pk 字节流恰好构成合法完整 msgp 数组的概率在工程上可忽略）。
func MemberPK(member string, hasCovering bool) string {
	if !hasCovering {
		if i := strings.LastIndex(member, "\x1f"); i > 0 {
			if _, err := strconv.ParseUint(member[i+1:], 10, 64); err == nil {
				return member[:i]
			}
		}
		return member
	}
	sz, rest, err := msgp.ReadArrayHeaderBytes([]byte(member))
	if err != nil || sz == 0 {
		return member
	}
	var pk string
	for i := uint32(0); i < sz; i++ {
		var v string
		v, rest, err = msgp.ReadStringBytes(rest)
		if err != nil {
			return member
		}
		if i == 0 {
			pk = v
		}
	}
	if len(rest) != 0 || pk == "" {
		return member // 非完整消费 → 按原始 pk
	}
	return pk
}

// MemberCovers 提取覆盖列（供覆盖索引读路径跳过回表，docs/03 §3.5）。
// 严格全量解析；失败返回 nil（调用方回退回表）。
func MemberCovers(member string, hasCovering bool) []string {
	if !hasCovering {
		return nil
	}
	sz, rest, err := msgp.ReadArrayHeaderBytes([]byte(member))
	if err != nil || sz == 0 {
		return nil
	}
	var covers []string
	for i := uint32(0); i < sz; i++ {
		if i == 1 {
			var skip string
			skip, rest, err = msgp.ReadStringBytes(rest)
			_ = skip
			if err != nil {
				return nil
			}
			continue
		}
		var v string
		v, rest, err = msgp.ReadStringBytes(rest)
		if err != nil {
			return nil
		}
		if i > 0 {
			covers = append(covers, v)
		}
	}
	if len(rest) != 0 {
		return nil
	}
	return covers
}

// MemberVer 提取 member 中的行版本戳（对账版本漂移方向，docs/12 §12.8）。
// 解析失败返回 (0, false)（畸形 member 由调用方按漂移处理）。
func MemberVer(member string, hasCovering bool) (uint64, bool) {
	if !hasCovering {
		i := strings.LastIndex(member, "\x1f")
		if i <= 0 {
			return 0, false
		}
		n, err := strconv.ParseUint(member[i+1:], 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	}
	sz, rest, err := msgp.ReadArrayHeaderBytes([]byte(member))
	if err != nil || sz < 2 {
		return 0, false
	}
	for i := uint32(0); i < 2; i++ {
		var v string
		v, rest, err = msgp.ReadStringBytes(rest)
		if err != nil {
			return 0, false
		}
		if i == 1 {
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return 0, false
			}
			return n, true
		}
	}
	return 0, false
}
