package kidb

import (
	"context"

	"kidb/script"
)

// KvClient 是内核与底层 Redis 集群客户端之间的唯一接口
// （docs/09 §9.3：实现方必须满足契约 R1~R7）。
//
// 参考实现：adapter/goredis（go-redis/v9 ClusterClient）。
// 私有客户端在各自仓库实现本接口，通过一致性测试套件（docs/12 §12.4）后接入。
type KvClient interface {
	// Do 执行单条命令。命令必须携带 key（契约 R2）。
	Do(ctx context.Context, cmd string, args ...any) (any, error)

	// Pipeline 执行一批可跨 slot 的命令，按节点聚合批量发出，
	// 按序返回结果（契约 R4）。无事务语义。
	Pipeline(ctx context.Context, cmds []Cmd) ([]any, error)

	// Eval 执行 Lua 脚本（EVALSHA 优先，NOSCRIPT 自动回退 EVAL，契约 R7）。
	// 调用方保证 numkeys≥1 且 keys[0] 携带目标 slot hash tag（契约 R3）。
	Eval(ctx context.Context, s *script.Script, keys []string, args ...any) (any, error)

	// Capabilities 声明可选能力（docs/09 §9.4）。
	Capabilities() Capabilities

	// DoReplica 仅当 Capabilities().ReplicaRead==true 时可用：
	// 只读命令路由到 slave/只读集群。
	DoReplica(ctx context.Context, cmd string, args ...any) (any, error)
}

// Cmd 是一条待 pipeline 的命令。
type Cmd struct {
	Name string
	Args []any
}

// Capabilities 是适配器的可选能力声明（nil channel = 不支持热 key 事件流）。
type Capabilities struct {
	ReplicaRead  bool          // L3 副本读
	HotkeyEvents <-chan string // 热 key 事件流
	ServerTime   bool          // TIME 命令可用
}
