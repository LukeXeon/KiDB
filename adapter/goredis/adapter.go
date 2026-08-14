// Package goredis 是 KiDB 的参考适配器：基于 go-redis/v9 ClusterClient
// 实现 kidb.Client 契约（docs/09 §9.3 R1~R7）。
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
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"kidb"
	"kidb/script"
)

// Adapter 在 *redis.ClusterClient 上实现 kidb.Client。
type Adapter struct {
	cli         *redis.ClusterClient
	replicaRead bool

	scriptsMu sync.Mutex
	scripts   map[string]*redis.Script // sha1 → 预编译脚本（EVALSHA 用）
}

// Options 是适配器构造参数（映射 kidb.Bootstrap 的连接部分）。
type Options struct {
	PoolSize     int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	// ReplicaRead 声明副本读能力（docs/09 §9.4）；开启后 go-redis 将只读
	// 命令路由到从节点。注意：设置 ClusterSlots 覆盖时 go-redis 会禁用
	// 该行为（其内部约束），测试环境 DoReplica 实际仍落主节点。
	ReplicaRead bool
	// ClusterSlots 覆盖拓扑获取（测试用 miniredis / 代理网关场景）；生产为 nil。
	ClusterSlots func(context.Context) ([]redis.ClusterSlot, error)
}

// New 构造适配器。返回的对象可直接作为 kidb.Client 使用。
func New(addrs []string, opt Options) *Adapter {
	cli := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        addrs,
		PoolSize:     opt.PoolSize,
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,
		ReadOnly:     opt.ReplicaRead,
		ClusterSlots: opt.ClusterSlots,
	})
	return &Adapter{
		cli:         cli,
		replicaRead: opt.ReplicaRead,
		scripts:     make(map[string]*redis.Script),
	}
}

// Close 关闭底层连接池。
func (a *Adapter) Close() error { return a.cli.Close() }

// Do 执行单条命令（契约 R2：命令必须携带 key；适配器做防御性校验）。
// redis.Nil 统一翻译为 (nil, nil)——"不存在"不是错误。
func (a *Adapter) Do(ctx context.Context, cmd string, args ...any) (any, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("%w: Do(%s) without key", kidb.ErrContractViolation, cmd)
	}
	res, err := a.cli.Do(ctx, append([]any{cmd}, args...)...).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return normalizeReply(res), err
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
func (a *Adapter) Pipeline(ctx context.Context, cmds []kidb.Cmd) ([]any, error) {
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
			results[i] = normalizeReply(v)
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
func (a *Adapter) Capabilities() kidb.Capabilities {
	return kidb.Capabilities{
		ReplicaRead:  a.replicaRead,
		HotkeyEvents: nil, // 开源参考适配器不提供热 key 事件流
		ServerTime:   true,
	}
}

// DoReplica 只读命令路由到 slave/只读副本（docs/09 §9.4）。
// 仅在构造时 ReplicaRead=true 时可用；内核须先查 Capabilities。
func (a *Adapter) DoReplica(ctx context.Context, cmd string, args ...any) (any, error) {
	if !a.replicaRead {
		return nil, fmt.Errorf("%w: DoReplica without ReplicaRead capability", kidb.ErrUnsupported)
	}
	return a.Do(ctx, cmd, args...)
}

// 编译期接口断言。
var _ kidb.Client = (*Adapter)(nil)
