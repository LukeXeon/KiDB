package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	query "github.com/dolthub/vitess/go/vt/proto/query"

	"kidb"
)

// typetext.go：Catalog 规范类型文本 → gms 类型的忠实重建（docs/02 §2.2
// 用户裁决：gms 内部规则零变更，类型忠实化使 MySQL 习惯校验天然兼容）。
//
// 文本空间 = gms Type.String() 对 DDL 白名单（ddlconvert.go ColumnTypeOf）
// 可能输出的全集；白名单外的文本意味着 Catalog 被写坏（唯一写入点是本包
// TableFromSchema），按契约违例报错。

// columnTypeFromText 按规范类型文本重建 gms 类型（varchar 长度不丢）。
func columnTypeFromText(text string) (sql.Type, error) {
	s := strings.ToLower(strings.TrimSpace(text))
	unsigned := false
	if strings.HasSuffix(s, " unsigned") {
		unsigned = true
		s = strings.TrimSuffix(s, " unsigned")
	}
	base, arg := s, -1
	if i := strings.IndexByte(s, '('); i >= 0 && strings.HasSuffix(s, ")") {
		base = s[:i]
		n, err := strconv.Atoi(s[i+1 : len(s)-1])
		if err != nil {
			return nil, fmt.Errorf("%w: 类型文本 %q 参数非法", kidb.ErrContractViolation, text)
		}
		arg = n
	}

	switch base {
	// 整数族（unsigned 变体由 gms String() 产出，如 "bigint unsigned"）
	case "tinyint":
		if arg > 0 { // display width（BOOL 别名的 tinyint(1)）
			return types.MustCreateNumberTypeWithDisplayWidth(sqltypes.Int8, arg), nil
		}
		return pick(unsigned, types.Uint8, types.Int8), nil
	case "smallint":
		return pick(unsigned, types.Uint16, types.Int16), nil
	case "mediumint":
		return pick(unsigned, types.Uint24, types.Int24), nil
	case "int":
		return pick(unsigned, types.Uint32, types.Int32), nil
	case "bigint":
		return pick(unsigned, types.Uint64, types.Int64), nil
	// 浮点
	case "float":
		return types.Float32, nil
	case "double":
		return types.Float64, nil
	// 字符串族（长度参数必需——gms String() 对这四类恒产出 (n)）
	case "char":
		return sizedString(sqltypes.Char, arg, text)
	case "varchar":
		return sizedString(sqltypes.VarChar, arg, text)
	case "tinytext":
		return types.TinyText, nil
	case "text":
		return types.Text, nil
	case "mediumtext":
		return types.MediumText, nil
	case "longtext":
		return types.LongText, nil
	// 二进制族
	case "binary":
		return sizedBinary(sqltypes.Binary, arg, text)
	case "varbinary":
		return sizedBinary(sqltypes.VarBinary, arg, text)
	case "tinyblob":
		return types.TinyBlob, nil
	case "blob":
		return types.Blob, nil
	case "mediumblob":
		return types.MediumBlob, nil
	case "longblob":
		return types.LongBlob, nil
	// 时间族（白名单只放行了 datetime/timestamp，docs/02 §2.3）
	case "datetime":
		return datetimeWithPrecision(sqltypes.Datetime, arg, text)
	case "timestamp":
		return datetimeWithPrecision(sqltypes.Timestamp, arg, text)
	// JSON
	case "json":
		return types.JSON, nil
	}
	return nil, fmt.Errorf("%w: 未知类型文本 %q（Catalog 只由 DDL 白名单写入）", kidb.ErrContractViolation, text)
}

func sizedString(base query.Type, n int, orig string) (sql.Type, error) {
	if n < 1 {
		return nil, fmt.Errorf("%w: 类型文本 %q 缺长度参数", kidb.ErrContractViolation, orig)
	}
	return types.MustCreateStringWithDefaults(base, int64(n)), nil
}

func sizedBinary(base query.Type, n int, orig string) (sql.Type, error) {
	if n < 1 {
		return nil, fmt.Errorf("%w: 类型文本 %q 缺长度参数", kidb.ErrContractViolation, orig)
	}
	return types.MustCreateBinary(base, int64(n)), nil
}

func pick[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

func datetimeWithPrecision(base query.Type, precision int, orig string) (sql.Type, error) {
	if precision < 0 {
		precision = 0
	}
	t, err := types.CreateDatetimeType(base, precision)
	if err != nil {
		return nil, fmt.Errorf("%w: 类型文本 %q 精度非法: %v", kidb.ErrContractViolation, orig, err)
	}
	return t, nil
}
