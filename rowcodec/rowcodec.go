// Package rowcodec 负责 gms 类型值 ↔ Redis 字符串编码的转换
// （docs/03 §3.4：标量字段 Hash 平铺零序列化；JSON 列当前存文本形态，
// TODO(impl): 切换 msgp 代码生成版，迁移走 docs/06 §6.4 版本纪律）。
package rowcodec

import (
	"fmt"
	"strconv"
	"time"

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
	return 0, fmt.Errorf("rowcodec: %v 不可作 score", ct)
}

// EncodeRow 把整行字段映射编码为存储形态（供 txguard.Fields）。
func EncodeRow(t *meta.TableDef, fields map[string]any) (map[string]string, error) {
	out := make(map[string]string, len(fields))
	for name, v := range fields {
		col, ok := t.Column(name)
		if !ok {
			return nil, fmt.Errorf("rowcodec: 未知列 %q", name)
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
	return row
}
