package engine

import (
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// sysvars.go：KiDB 语义开关 = gms 原生系统变量（docs/02 §2.7、docs/10 §10.2）。
//
// 数据流：SET GLOBAL → gms 校验/作用域处理 → 本实例全局值生效 →
// NotifyChanged 钩子经会话携带的 Cfg 句柄持久化 cfg:global（CAS + _ver）→
// 其他实例的版本轮询把远端变更回填各自的 gms 注册表（装配层负责，见 gateway）。
// 读侧（闸门/开关消费方）一律读本实例 gms 注册表——单一事实源。

const (
	// VarFullscanAllowlist 全表遍历表白名单（逃生门，逗号分隔，docs/07 §7.4）。
	VarFullscanAllowlist = "query_allow_fullscan_tables"
	// VarReplicaRead L3 副本读开关（docs/09 §9.4）。
	VarReplicaRead = "replica_read"
	// VarRowCache 行级近缓存开关（docs/08 §8.4）。
	VarRowCache = "hotkey_row_cache"
)

var registerSysvarsOnce sync.Once

// RegisterSysvars 注册 3 个语义开关（幂等——测试可多次 Build）。
// 全部 Dynamic + Global 作用域（集群级语义开关无会话级意义；SET SESSION
// 由 gms 按 GlobalOnly 语义拒绝）。
func RegisterSysvars() {
	registerSysvarsOnce.Do(func() {
		sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
			&sql.MysqlSystemVariable{
				Name:    VarFullscanAllowlist,
				Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Global),
				Dynamic: true,
				Type:    types.NewSystemStringType(VarFullscanAllowlist),
				Default: "",
				NotifyChanged: notifyPersist(VarFullscanAllowlist, func(v any) string {
					return strings.ToLower(strings.TrimSpace(fmt.Sprint(v)))
				}),
			},
			&sql.MysqlSystemVariable{
				Name:          VarReplicaRead,
				Scope:         sql.GetMysqlScope(sql.SystemVariableScope_Global),
				Dynamic:       true,
				Type:          types.NewSystemBoolType(VarReplicaRead),
				Default:       int8(0),
				NotifyChanged: notifyPersist(VarReplicaRead, boolVarText),
			},
			&sql.MysqlSystemVariable{
				Name:          VarRowCache,
				Scope:         sql.GetMysqlScope(sql.SystemVariableScope_Global),
				Dynamic:       true,
				Type:          types.NewSystemBoolType(VarRowCache),
				Default:       int8(0),
				NotifyChanged: notifyPersist(VarRowCache, boolVarText),
			},
		})
	})
}

// notifyPersist 生成 SET GLOBAL 持久化钩子：ro 执法（RejectRO）→
// 经会话 Cfg 句柄 CAS 落 cfg:global。会话无 Cfg（内部种子/回填路径——
// 值本就来自 cfg:global）跳过持久化。
func notifyPersist(name string, toText func(any) string) func(*sql.Context, sql.SystemVariableScope, sql.SystemVarValue) error {
	return func(ctx *sql.Context, scope sql.SystemVariableScope, svv sql.SystemVarValue) error {
		if ms, ok := scope.(*sql.MysqlScope); !ok || ms.Type != sql.SystemVariableScope_Global {
			return nil // 非 Global 作用域变更不落盘
		}
		if err := RejectRO(ctx); err != nil {
			return sqlErr(err)
		}
		cfg := SessionCfg(ctx)
		if cfg == nil {
			return nil // 内部路径（启动种子/远端回填）：值源自 cfg:global，无需回写
		}
		return sqlErr(cfg.Set(ctx, name, toText(svv.Val)))
	}
}

// boolVarText 布尔变量值 → 存储文本（"true"/"false"，与 config 校验器口径一致）。
func boolVarText(v any) string {
	switch b := v.(type) {
	case bool:
		if b {
			return "true"
		}
		return "false"
	case int8:
		if b != 0 {
			return "true"
		}
		return "false"
	}
	return fmt.Sprint(v)
}

// SysvarBool 读本实例全局布尔开关（单一事实源 = gms 注册表）。
func SysvarBool(name string) bool {
	_, v, ok := sql.SystemVariables.GetGlobal(name)
	if !ok {
		return false
	}
	switch b := v.(type) {
	case bool:
		return b
	case int8:
		return b != 0
	}
	return false
}

// SysvarString 读本实例全局字符串开关。
func SysvarString(name string) string {
	_, v, ok := sql.SystemVariables.GetGlobal(name)
	if !ok {
		return ""
	}
	return fmt.Sprint(v)
}
