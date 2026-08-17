package kidb

import "time"

// Bootstrap：进程级引导配置，不可共享，经 NewKernel 注入
// （docs/10 §10.4：网关进程由启动参数/环境变量构造；
// 测试与带外工具直接以代码构造）。
type Bootstrap struct {
	Addrs        []string      // Redis Cluster 地址
	PoolSize     int           // 连接池
	ReadTimeout  time.Duration // 契约 R6；scatter 预算 = ReadTimeout × headroom
	WriteTimeout time.Duration
	ReplicaRead  bool      // 可选能力：副本读（L3）
	ListenAddr   string    // 网关监听地址（cmd/kidb-server）
	Accounts     []Account // 账号表（docs/02 §2.9：两级权限 rw/ro）
	TLSCertFile  string    // 可选 TLS（docs/02 §2.1）
	TLSKeyFile   string
	Lang         string // 用户面向消息语言（"en" 默认 / "zh"，docs/10 §10.1 i18n 选型）
}

// Account 是一个网关账号。Role ∈ {"rw","ro"}；ro 执行写/DDL/SET GLOBAL
// 报 ErrReadOnly（映射 MySQL 1290）。
type Account struct {
	User     string
	Host     string // MySQL 风格的 host 匹配，'%' 为通配
	Password string // mysql_native_password
	Role     string
}
