package engine

import (
	"errors"

	"github.com/dolthub/vitess/go/mysql"
	goerrors "gopkg.in/src-d/go-errors.v1"

	"kidb"
)

// sqlerr.go：内核错误 → MySQL 错误码的翻译（docs/02 §2.8 映射表）。
// v6.0 无网关包装层，本函数是映射的唯一落点：gms CastSQLError 对
// *mysql.SQLError 原样透传、对 gms 原生 kind 错误自识别，其余一律 1105。

// sqlErr 把引擎边界（编辑器/DDL/闸门/sysvar 钩子）返回的内核错误翻译为
// 带 MySQL 错误码的形态。gms 原生错误（go-errors kind，含唯一冲突携带的
// Existing 行结构——IGNORE/ODKU 分流依赖）原样透传。
func sqlErr(err error) error {
	if err == nil {
		return nil
	}
	var se *mysql.SQLError
	if errors.As(err, &se) {
		return err // 已是带码形态
	}
	var ge *goerrors.Error
	if errors.As(err, &ge) {
		return err // gms 原生 kind 错误（CastSQLError 自识别）
	}
	return mysql.NewSQLError(kidb.MySQLCode(err), "HY000", "%s", err.Error())
}
