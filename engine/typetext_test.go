package engine

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"
)

// TestColumnTypeFromTextRoundTrip 忠实类型往返：gms 解析产物 String() →
// Catalog 文本 → 重建 → String() 恒等（docs/02 §2.2 用户裁决的核心不变式）。
func TestColumnTypeFromTextRoundTrip(t *testing.T) {
	srcs := []interface{ String() string }{
		types.Int8, types.Int16, types.Int24, types.Int32, types.Int64,
		types.Uint8, types.Uint16, types.Uint24, types.Uint32, types.Uint64,
		types.Float32, types.Float64,
		types.MustCreateStringWithDefaults(sqltypes.Char, 8),
		types.MustCreateStringWithDefaults(sqltypes.VarChar, 32),
		types.TinyText, types.Text, types.MediumText, types.LongText,
		types.MustCreateBinary(sqltypes.Binary, 16),
		types.MustCreateBinary(sqltypes.VarBinary, 64),
		types.TinyBlob, types.Blob, types.MediumBlob, types.LongBlob,
		types.Datetime, types.Timestamp,
		types.JSON,
		types.Boolean, // tinyint(1) display-width 保留
	}
	for _, src := range srcs {
		text := src.String()
		back, err := columnTypeFromText(text)
		require.NoError(t, err, text)
		require.Equal(t, text, back.String(), "往返必须恒等: %s", text)
	}
}

// TestColumnTypeFromTextRejects 白名单外文本 = Catalog 写坏（契约违例）。
func TestColumnTypeFromTextRejects(t *testing.T) {
	for _, bad := range []string{"", "decimal(10,2)", "date", "enum('a','b')", "varchar", "varchar(x)"} {
		_, err := columnTypeFromText(bad)
		require.Error(t, err, bad)
	}
}
