// Package rowcodec 负责 gms 类型值 ↔ Redis 字符串编码的转换
// （docs/03 §3.4：标量字段 Hash 平铺零序列化；JSON 列保留文本形态——
// 文本即 JSON 标准格式，msgp 压缩收益在列内文档场景，经 _fmtv 演进）。
package rowcodec

import (
	"strings"
	"fmt"
	"strconv"
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
		if s, ok := v.(string); ok {
			return s, nil // v1 文本形态
		}
		return fmt.Sprint(v), nil
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
		return s, nil
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

// ==== 桶 member 编码（docs/03 §3.4/§3.5）====
// 无覆盖列：member = 原始 pk（零序列化红利）。
// 有覆盖列：member = msgp 数组 [pk, cover1, ...]（代码生成级格式，二进制安全——
// 替代 v1 的 "|" 拼接，根除 pk/值含分隔符的歧义风险）。
// 读取侧经索引定义得知是否有覆盖列（schema 感知，无格式猜测）。

// LexMember 字典序副本 member：value + \x00 + pk（按值字典序再按 pk 唯一）。
// txguard 写路径与 JobRunner 回填共用（编码单点）。
func LexMember(value, pk string) string {
	return value + "\x00" + pk
}

// EncodeMember 生成桶 member。
func EncodeMember(pk string, covers []string) string {
	if len(covers) == 0 {
		return pk
	}
	b := msgp.AppendArrayHeader(nil, uint32(1+len(covers)))
	b = msgp.AppendString(b, pk)
	for _, c := range covers {
		b = msgp.AppendString(b, c)
	}
	return string(b)
}

// MemberPK 提取 member 中的 pk。hasCovering 来自索引定义（schema 感知）。
// 严格全量解析（所有元素消费完毕才算 msgp 形态），任一失败回退原样——
// 原始 pk 字节流恰好构成合法完整 msgp 数组的概率在工程上可忽略。
func MemberPK(member string, hasCovering bool) string {
	if !hasCovering {
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
