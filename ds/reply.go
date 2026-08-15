package ds

import "fmt"

// reply.go：KvClient 泛化 Do/Pipeline 回复的形态归一（契约 docs/09 §9.3：
// 适配器对 Hash 类回复可返回 map[string]string 或 map[any]any，
// 数组类可返回 []string 或 []any——归一是全内核共享的小工具）。

// Strings 归一 ZRANGE 族返回为字符串切片。
func Strings(res any) []string {
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

// StringMap 归一 HGETALL 返回为 map[string]string（map 或扁平数组两形态）。
// 扁平数组奇数长度 = 回复截断（契约违例）报错；nil = 空 map。
func StringMap(res any) (map[string]string, error) {
	switch v := res.(type) {
	case map[string]string:
		return v, nil
	case map[any]any:
		m := make(map[string]string, len(v))
		for k, val := range v {
			m[fmt.Sprint(k)] = fmt.Sprint(val)
		}
		return m, nil
	case []any:
		if len(v)%2 != 0 {
			return nil, fmt.Errorf("hgetall 回复奇数长度 %d（截断）", len(v))
		}
		m := make(map[string]string, len(v)/2)
		for i := 0; i < len(v); i += 2 {
			m[fmt.Sprint(v[i])] = fmt.Sprint(v[i+1])
		}
		return m, nil
	case nil:
		return map[string]string{}, nil
	}
	return nil, fmt.Errorf("hgetall 回复形态未知 %T", res)
}
