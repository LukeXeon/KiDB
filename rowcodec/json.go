package rowcodec

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/tinylib/msgp/msgp"

	"kidb/meta"
)

// json.go：JSON 列的二进制存储形态（docs/03 §3.4）——文本解析后按 msgpack
// 编码（体积优化；key 排序归一——map 序不保真是 Go/JSON 双侧的常态，
// MySQL 二进制 JSON 同样归一化键序）。
//
// 精度声明：JSON 数字按 float64 归一（与 MySQL 二进制 JSON 的 DOUBLE 归一
// 同族）；超过 2^53 的整数精度不担保（docs/03 §3.4 score 同款纪律）。

// EncodeJSON 把 gms JSON 列值编码为 msgpack 二进制形态。
// 输入形态：types.JSONDocument（gms Convert 产物）/ string / []byte / 已解析值。
func EncodeJSON(v any) (string, error) {
	var doc any
	switch t := v.(type) {
	case nil:
		return "", nil
	case interface{ ToInterface() (interface{}, error) }: // sql.JSONWrapper（types.JSONDocument）
		d, err := t.ToInterface()
		if err != nil {
			return "", err
		}
		doc = d
	case string:
		if err := json.Unmarshal([]byte(t), &doc); err != nil {
			return "", fmt.Errorf("rowcodec: JSON 文本非法: %w", err)
		}
	case []byte:
		if err := json.Unmarshal(t, &doc); err != nil {
			return "", fmt.Errorf("rowcodec: JSON 文本非法: %w", err)
		}
	default:
		doc = t
	}
	out, err := appendJSONMsgp(nil, doc)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// DecodeJSON 把 msgpack 二进制还原为 JSON 文本（key 排序归一）。
func DecodeJSON(s string) (any, error) {
	v, _, err := readJSONMsgp([]byte(s))
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(v) // encoding/json 对 map key 排序输出
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

// appendJSONMsgp 递归把 JSON 值写为 msgpack（map key 排序）。
func appendJSONMsgp(dst []byte, v any) ([]byte, error) {
	switch t := v.(type) {
	case nil:
		return msgp.AppendNil(dst), nil
	case bool:
		return msgp.AppendBool(dst, t), nil
	case string:
		return msgp.AppendString(dst, t), nil
	case float64:
		return msgp.AppendFloat64(dst, t), nil
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			return nil, fmt.Errorf("rowcodec: JSON 数字 %q 不可归一: %w", t, err)
		}
		return msgp.AppendFloat64(dst, f), nil
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		dst = msgp.AppendMapHeader(dst, uint32(len(keys)))
		for _, k := range keys {
			dst = msgp.AppendString(dst, k)
			var err error
			dst, err = appendJSONMsgp(dst, t[k])
			if err != nil {
				return nil, err
			}
		}
		return dst, nil
	case []any:
		dst = msgp.AppendArrayHeader(dst, uint32(len(t)))
		for _, e := range t {
			var err error
			dst, err = appendJSONMsgp(dst, e)
			if err != nil {
				return nil, err
			}
		}
		return dst, nil
	}
	return nil, fmt.Errorf("rowcodec: JSON 值类型 %T 不可编码", v)
}

// readJSONMsgp 读一个 msgpack 值，返回 (值, 剩余, 错误)。
func readJSONMsgp(b []byte) (any, []byte, error) {
	switch msgp.NextType(b) {
	case msgp.NilType:
		rest, err := msgp.ReadNilBytes(b)
		return nil, rest, err
	case msgp.BoolType:
		v, rest, err := msgp.ReadBoolBytes(b)
		return v, rest, err
	case msgp.StrType:
		v, rest, err := msgp.ReadStringBytes(b)
		return v, rest, err
	case msgp.Float64Type, msgp.IntType, msgp.UintType:
		// JSON 数字统一 float64（归一纪律见头注）；整型原样读再转
		if msgp.NextType(b) == msgp.Float64Type {
			v, rest, err := msgp.ReadFloat64Bytes(b)
			return v, rest, err
		}
		if msgp.NextType(b) == msgp.IntType {
			v, rest, err := msgp.ReadInt64Bytes(b)
			return float64(v), rest, err
		}
		v, rest, err := msgp.ReadUint64Bytes(b)
		return float64(v), rest, err
	case msgp.MapType:
		n, rest, err := msgp.ReadMapHeaderBytes(b)
		if err != nil {
			return nil, nil, err
		}
		m := make(map[string]any, n)
		for i := uint32(0); i < n; i++ {
			var k string
			k, rest, err = msgp.ReadStringBytes(rest)
			if err != nil {
				return nil, nil, err
			}
			var v any
			v, rest, err = readJSONMsgp(rest)
			if err != nil {
				return nil, nil, err
			}
			m[k] = v
		}
		return m, rest, nil
	case msgp.ArrayType:
		n, rest, err := msgp.ReadArrayHeaderBytes(b)
		if err != nil {
			return nil, nil, err
		}
		a := make([]any, 0, n)
		for i := uint32(0); i < n; i++ {
			var v any
			v, rest, err = readJSONMsgp(rest)
			if err != nil {
				return nil, nil, err
			}
			a = append(a, v)
		}
		return a, rest, nil
	}
	return nil, nil, fmt.Errorf("rowcodec: 未知 msgpack 形态")
}

// 引用保活（meta 供常量对齐）。
var _ = meta.ColJSON
