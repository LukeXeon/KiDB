// Package goredis 是 KiDB 的参考适配器：基于 go-redis/v9 ClusterClient
// 实现 kv.Client 契约（docs/09 §9.3 R1~R7）。
//
// 路由说明：go-redis ClusterClient 对内建命令表中的命令按首 key 路由
// （其 CRC16 与 keycodec 同为 XMODEM 规范，一致性由契约测试在真实集群校验，
// docs/12 §12.4）；EVAL 按 keys[0] 路由；MOVED/ASK 由 go-redis 自动跟随并
// 刷新拓扑（R5），耗尽后的错误经 errors.Is 可识别，由内核按
// docs/09 §9.6 退避矩阵处理。
//
// 已知边界：带外运维命令（MEMORY USAGE 等）不经本适配器的泛化 Do 下发
// （go-redis 泛化命令对未知命令的路由不可控），带外工具用类型化客户端直连。
package goredis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"kidb"
	"kidb/kv"
	"kidb/script"
)

// Adapter 在 *redis.ClusterClient 上实现 kv.Client。
//
// 副本读（docs/09 §9.4 可选能力）：ReplicaRead 开启时构造第二个
// ClusterClient（ReadOnly=true，go-redis 把只读命令路由到从节点）——
// 主客户端保持主节点路由（元数据/写邻读的主节点纪律不动），
// DoReplica/PipelineReplica 走副本客户端。注意：设置 ClusterSlots 覆盖时
// go-redis 会禁用副本路由（其内部约束），测试环境副本读实际仍落主节点。
type Adapter struct {
	cli     *redis.ClusterClient
	replica *redis.ClusterClient // 非 nil = 副本读可用

	scriptsMu sync.Mutex
	scripts   map[string]*redis.Script // sha1 → 预编译脚本（EVALSHA 用）
}

// Options 是适配器构造参数（映射 kidb.Bootstrap 的连接部分）。
type Options struct {
	PoolSize     int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	// ReplicaRead 声明副本读能力（docs/09 §9.4）：开启后 DoReplica /
	// PipelineReplica 经独立 ReadOnly 客户端把只读命令路由到从节点。
	ReplicaRead bool
	// ClusterSlots 覆盖拓扑获取（测试用 miniredis / 代理网关场景）；生产为 nil。
	ClusterSlots func(context.Context) ([]redis.ClusterSlot, error)
}

// New 构造适配器。返回的对象可直接作为 kv.Client 使用。
//
// 协议钉住 RESP2（docs/09 §9.2）：go-redis v9.22 起未显式设置的 Protocol
// 被静默改为 RESP3（options.go init: <2→3）——RESP3 激活 push 通知处理器，
// 读回复前 PeekReplyType 的竞态曾实证挂死测试（miniredis），且 HELLO 3 对
// RESP2-only 代理/老服务端直接断连。KiDB 不消费任何 push 语义，显式钉 2。
func New(addrs []string, opt Options) *Adapter {
	base := &redis.ClusterOptions{
		Addrs:        addrs,
		PoolSize:     opt.PoolSize,
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,
		ClusterSlots: opt.ClusterSlots,
		Protocol:     2,
	}
	a := &Adapter{
		cli:     redis.NewClusterClient(base),
		scripts: make(map[string]*redis.Script),
	}
	if opt.ReplicaRead {
		ro := *base
		ro.ReadOnly = true // 只读命令路由到从节点（go-redis 内建命令表判定）
		a.replica = redis.NewClusterClient(&ro)
	}
	return a
}

// Close 关闭底层连接池。
func (a *Adapter) Close() error {
	if a.replica != nil {
		_ = a.replica.Close()
	}
	return a.cli.Close()
}

// Do 执行单条命令（契约 R2：命令必须携带 key；适配器做防御性校验）。
// redis.Nil 统一翻译为 (nil, nil)——"不存在"不是错误。
// 例外：TIME 是运维带外命令（docs/09 §9.2 白名单），无 key 放行
// （go-redis 将其路由到任一节点，时钟读取语义成立）。
func (a *Adapter) Do(ctx context.Context, cmd string, args ...any) (any, error) {
	if len(args) == 0 && !strings.EqualFold(cmd, "TIME") {
		return nil, fmt.Errorf("%w: Do(%s) without key", kidb.ErrContractViolation, cmd)
	}
	res, err := a.cli.Do(ctx, append([]any{cmd}, args...)...).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return normalizeCmdReply(cmd, res), err
}

// normalizeCmdReply 按命令名归一泛化回复（契约 docs/09 §9.3 R2）：
// Hash 类回复（HGETALL）统一为 map[string]string——RESP2 下 go-redis 泛化 Do
// 对 HGETALL 返回扁平数组（RESP3 才给 map），由适配器补齐契约形态。
func normalizeCmdReply(cmd string, v any) any {
	if arr, ok := v.([]any); ok && strings.EqualFold(cmd, "HGETALL") && len(arr)%2 == 0 {
		m := make(map[string]string, len(arr)/2)
		for i := 0; i < len(arr); i += 2 {
			m[fmt.Sprint(arr[i])] = fmt.Sprint(arr[i+1])
		}
		return m
	}
	return normalizeReply(v)
}

// normalizeReply 归一 go-redis 泛化命令的宽松返回形态：
// Hash 类回复统一为 map[string]string（内核消费者只认契约形态）。
func normalizeReply(v any) any {
	switch m := v.(type) {
	case map[any]any:
		out := make(map[string]string, len(m))
		for k, val := range m {
			out[fmt.Sprint(k)] = fmt.Sprint(val)
		}
		return out
	case []any:
		out := make([]any, len(m))
		for i, e := range m {
			out[i] = normalizeReply(e)
		}
		return out
	}
	return v
}

// Pipeline 执行一批可跨 slot 的命令（契约 R4：go-redis 按节点聚合、
// 按序返回；无事务语义）。元素级 redis.Nil 翻译为 nil；
// 首个非 Nil 错误作为整体错误返回，结果切片仍填满。
func (a *Adapter) Pipeline(ctx context.Context, cmds []kv.Cmd) ([]any, error) {
	if len(cmds) == 0 {
		return nil, nil
	}
	pipe := a.cli.Pipeline()
	cmder := make([]*redis.Cmd, len(cmds))
	for i, c := range cmds {
		if len(c.Args) == 0 {
			return nil, fmt.Errorf("%w: Pipeline cmd %s without key", kidb.ErrContractViolation, c.Name)
		}
		cmder[i] = pipe.Do(ctx, append([]any{c.Name}, c.Args...)...)
	}
	_, execErr := pipe.Exec(ctx)
	results := make([]any, len(cmds))
	var firstErr error
	for i, c := range cmder {
		v, err := c.Result()
		switch {
		case errors.Is(err, redis.Nil):
			results[i] = nil
		case err != nil:
			results[i] = nil
			if firstErr == nil {
				firstErr = err
			}
		default:
			results[i] = normalizeCmdReply(cmds[i].Name, v)
		}
	}
	if firstErr != nil {
		return results, firstErr
	}
	if errors.Is(execErr, redis.Nil) {
		return results, nil
	}
	return results, execErr
}

// Eval 执行 Lua 脚本（契约 R7：EVALSHA 优先，NOSCRIPT 自动回退 EVAL——
// 由 go-redis Script.Run 实现，docs/05 §5.7）。
func (a *Adapter) Eval(ctx context.Context, s *script.Script, keys []string, args ...any) (any, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: Eval(%s) numkeys=0", kidb.ErrContractViolation, s.Name)
	}
	sc := a.cachedScript(s)
	res, err := sc.Run(ctx, a.cli, keys, args...).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return res, err
}

func (a *Adapter) cachedScript(s *script.Script) *redis.Script {
	a.scriptsMu.Lock()
	defer a.scriptsMu.Unlock()
	if sc, ok := a.scripts[s.SHA1]; ok {
		return sc
	}
	sc := redis.NewScript(s.Src)
	a.scripts[s.SHA1] = sc
	return sc
}

// Capabilities 声明可选能力（docs/09 §9.4）。
func (a *Adapter) Capabilities() kv.Capabilities {
	return kv.Capabilities{
		ReplicaRead:  a.replica != nil,
		HotkeyEvents: nil, // 开源参考适配器不提供热 key 事件流
		ServerTime:   true,
	}
}

// DoReplica 只读命令路由到 slave/只读副本（docs/09 §9.4）。
// 仅在构造时 ReplicaRead=true 时可用；内核须先查 Capabilities。
func (a *Adapter) DoReplica(ctx context.Context, cmd string, args ...any) (any, error) {
	if a.replica == nil {
		return nil, fmt.Errorf("%w: DoReplica without ReplicaRead capability", kidb.ErrUnsupported)
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("%w: DoReplica(%s) without key", kidb.ErrContractViolation, cmd)
	}
	res, err := a.replica.Do(ctx, append([]any{cmd}, args...)...).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return normalizeCmdReply(cmd, res), err
}

// PipelineReplica 批级副本读（契约 R4 同形，路由到从节点）。
func (a *Adapter) PipelineReplica(ctx context.Context, cmds []kv.Cmd) ([]any, error) {
	if a.replica == nil {
		return nil, fmt.Errorf("%w: PipelineReplica without ReplicaRead capability", kidb.ErrUnsupported)
	}
	if len(cmds) == 0 {
		return nil, nil
	}
	pipe := a.replica.Pipeline()
	cmder := make([]*redis.Cmd, len(cmds))
	for i, c := range cmds {
		if len(c.Args) == 0 {
			return nil, fmt.Errorf("%w: PipelineReplica cmd %s without key", kidb.ErrContractViolation, c.Name)
		}
		cmder[i] = pipe.Do(ctx, append([]any{c.Name}, c.Args...)...)
	}
	_, execErr := pipe.Exec(ctx)
	results := make([]any, len(cmds))
	var firstErr error
	for i, c := range cmder {
		v, err := c.Result()
		switch {
		case errors.Is(err, redis.Nil):
			results[i] = nil
		case err != nil:
			results[i] = nil
			if firstErr == nil {
				firstErr = err
			}
		default:
			results[i] = normalizeCmdReply(cmds[i].Name, v)
		}
	}
	if firstErr != nil {
		return results, firstErr
	}
	if errors.Is(execErr, redis.Nil) {
		return results, nil
	}
	return results, execErr
}

// 编译期接口断言。
var _ kv.Client = (*Adapter)(nil)
