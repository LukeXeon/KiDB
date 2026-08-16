package rowcodec

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestJSONRoundTrip 归一化文本直存往返：语义等价 + key 排序归一 + 数字归一。
func TestJSONRoundTrip(t *testing.T) {
	cases := []struct{ in, want string }{
		{`{"a":1,"b":"x"}`, `{"a":1,"b":"x"}`},
		{`{"b":"x","a":1}`, `{"a":1,"b":"x"}`}, // key 归一
		{`{"n":[1,2.5,true,null,"s"],"o":{"z":0}}`, `{"n":[1,2.5,true,null,"s"],"o":{"z":0}}`},
		{`[1,2,3]`, `[1,2,3]`},
		{`"str"`, `"str"`},
		{`123`, `123`},
		{`1.0`, `1`}, // float64 最短形式（如实声明的展示级差异）
		{`true`, `true`},
		{`null`, `null`},
		{`{"a":1,"a":2}`, `{"a":2}`}, // 重复 key 后写胜（MySQL 二进制 JSON 同族）
	}
	for _, c := range cases {
		enc, err := EncodeJSON(c.in)
		require.NoError(t, err, c.in)
		require.Equal(t, c.want, enc, c.in) // 存储形态即规范文本
		dec, err := DecodeJSON(enc)
		require.NoError(t, err, c.in)
		require.Equal(t, c.want, dec, c.in) // 读出 = 存储形态
	}
}

// TestJSONTypes 输入形态：gms JSONWrapper（ToInterface）/ []byte / 已解析值。
func TestJSONTypes(t *testing.T) {
	a, err := EncodeJSON(`{"k":1}`)
	require.NoError(t, err)
	b, err := EncodeJSON([]byte(`{"k":1}`))
	require.NoError(t, err)
	require.Equal(t, a, b)
	c, err := EncodeJSON(map[string]any{"k": 1.0})
	require.NoError(t, err)
	require.Equal(t, a, c)
	d, err := EncodeJSON(nil)
	require.NoError(t, err)
	require.Empty(t, d)

	// 非法 JSON 文本报错
	_, err = EncodeJSON(`{"bad`)
	require.Error(t, err)
}

// TestJSONOpsReadable 运维可观测性（裁决依据之一）：存储形态 HGET 直读为文本。
func TestJSONOpsReadable(t *testing.T) {
	enc, err := EncodeJSON(`{"order_id":"x","qty":2}`)
	require.NoError(t, err)
	require.True(t, jsonValid(enc), "存储形态必须是可直读文本 JSON")
	require.Contains(t, enc, `"order_id"`)
}

func jsonValid(s string) bool {
	var v any
	return json.Unmarshal([]byte(s), &v) == nil
}
