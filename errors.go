package kidb

import (
	"errors"

	"kidb/kv"
)

// 内核错误码（docs/02 §2.9：MySQL 错误码映射表的唯一实现落点）。
var (
	ErrDuplicateKey      = errors.New("ERR_DUPLICATE_KEY")      // 唯一冲突 → 1062
	ErrUnsupported       = errors.New("ERR_UNSUPPORTED")        // 事务/TRUNCATE/GRANT/超范围语法 → 1235
	ErrUnsupportedJoin   = errors.New("ERR_UNSUPPORTED_JOIN")   // 档 4 JOIN → 1235
	ErrNoIndex           = errors.New("ERR_NO_INDEX")           // 无索引谓词且未开全扫
	ErrRowTooLarge       = errors.New("ERR_ROW_TOO_LARGE")      // 超 max_row_bytes
	ErrIndexLogFull      = errors.New("ERR_INDEX_LOG_FULL")     // 异步索引日志背压
	ErrStaleMetadata     = errors.New("ERR_STALE_METADATA")     // 版本冲突重试耗尽 → 1197
	ErrReadOnly          = errors.New("ERR_READ_ONLY")          // 只读账号写操作 → 1290
	ErrCapability        = errors.New("ERR_CAPABILITY")         // 能力探测失败（启动期）
	ErrContractViolation = errors.New("ERR_CONTRACT_VIOLATION") // CROSSSLOT 等契约违例（内核 bug，不重试）
)

// MySQLCode 返回内核错误对应的 MySQL 错误码（docs/02 §2.9）。
func MySQLCode(err error) int {
	switch {
	case errors.Is(err, ErrDuplicateKey):
		return 1062
	case errors.Is(err, ErrUnsupported), errors.Is(err, ErrUnsupportedJoin):
		return 1235
	case errors.Is(err, ErrStaleMetadata):
		return 1197
	case errors.Is(err, kv.ErrRedirectExhausted), errors.Is(err, kv.ErrClusterUnavailable):
		return 1105
	case errors.Is(err, ErrReadOnly):
		return 1290
	default:
		return 1105 // ER_UNKNOWN_ERROR 兜底
	}
}
