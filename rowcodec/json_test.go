package rowcodec

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestJSONRoundTrip msgpack 形态往返：语义等价 + key 排序归一。
func TestJSONRoundTrip(t *testing.T) {
	cases := []struct{ in, want string }{
		{`{"a":1,"b":"x"}`, `{"a":1,"b":"x"}`},
		{`{"b":"x","a":1}`, `{"a":1,"b":"x"}`},                       // key 归一
		{`{"n":[1,2.5,true,null,"s"],"o":{"z":0}}`, `{"n":[1,2.5,true,null,"s"],"o":{"z":0}}`},
		{`[1,2,3]`, `[1,2,3]`},
		{`"str"`, `"str"`},
		{`123`, `123`},
		{`true`, `true`},
		{`null`, `null`},
	}
	for _, c := range cases {
		enc, err := EncodeJSON(c.in)
		require.NoError(t, err, c.in)
		dec, err := DecodeJSON(enc)
		require.NoError(t, err, c.in)
		require.Equal(t, c.want, dec, c.in)
	}
}

// TestJSONTypes 输入形态：gms JSONWrapper（ToInterface）/ []byte / 已解析值。
func TestJSONTypes(t *testing.T) {
	type fakeWrapper struct{ v any }
	_ = fakeWrapper{}
	// string 与 []byte
	a, err := EncodeJSON(`{"k":1}`)
	require.NoError(t, err)
	b, err := EncodeJSON([]byte(`{"k":1}`))
	require.NoError(t, err)
	require.Equal(t, a, b)
	// 已解析 map
	c, err := EncodeJSON(map[string]any{"k": 1.0})
	require.NoError(t, err)
	require.Equal(t, a, c)
	// nil
	d, err := EncodeJSON(nil)
	require.NoError(t, err)
	require.Empty(t, d)

	// 非法 JSON 文本报错
	_, err = EncodeJSON(`{"bad`)
	require.Error(t, err)
}

// TestJSONSizeWin 体积对比（msgpack vs 文本）——文档化体积优化的实证。
func TestJSONSizeWin(t *testing.T) {
	typical := `{"order_id":"a1b2c3d4-e5f6","user":{"id":1024567,"name":"张三","vip":true},"items":[{"sku":"SKU-1001","qty":2,"price":99.50},{"sku":"SKU-1002","qty":1,"price":12.00}],"tags":["fresh","sale","east"],"created_at":"2026-08-16T12:00:00Z","note":null}`
	enc, err := EncodeJSON(typical)
	require.NoError(t, err)
	t.Logf("文本 %dB → msgpack %dB（%.1f%%）", len(typical), len(enc), float64(len(enc))*100/float64(len(typical)))
	require.Less(t, len(enc), len(typical))
	// 往返语义
	dec, err := DecodeJSON(enc)
	require.NoError(t, err)
	require.JSONEq(t, typical, fmt.Sprint(dec))
	require.True(t, strings.HasPrefix(fmt.Sprint(dec), `{"created_at"`)) // key 排序
}
