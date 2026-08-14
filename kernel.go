// Package kidb 是 KiDB 内核的根包：内核组装（Kernel/Querier）、
// 后端可替换契约（Client）、引导配置（Bootstrap）与错误码。
//
// 设计文档：docs/01-定位架构与TiDB对齐.md、docs/02-SQL服务器.md、
// docs/09-后端契约与适配器.md。文档即接口契约，代码与文档一一对应。
package kidb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"kidb/script"
)

// Querier 是内核对外暴露的唯一接口（docs/01 §1.6），签名对齐
// go-mysql-server 引擎（docs/02 §2.5 DML 路径）。
// 产品形态只有网关一种；Querier 同时作为测试与带外工具的程序化入口
// （工程接缝，非第二产品形态）。
type Querier interface {
	Query(ctx context.Context, query string) (sql.Schema, sql.RowIter, error)
	Exec(ctx context.Context, query string) (types.OkResult, error)
}

// Kernel 是内核组装体。构造见 NewKernel。
type Kernel struct {
	cli      Client
	boot     Bootstrap
	logger   *slog.Logger
	scripts  *script.Registry
	closed   chan struct{}
	closeErr error
}

// Option 是内核可选注入项（docs/10 §10.4：指标与日志经接口注入）。
type Option func(*Kernel)

// WithLogger 注入结构化日志 Handler。
func WithLogger(h slog.Handler) Option {
	return func(k *Kernel) { k.logger = slog.New(h) }
}

// NewKernel 注入 Client 与进程级引导配置，组装内核。
//
// 启动期执行（docs/09 §9.4、docs/05 §5.7）：
//  1. Lua 资产加载与静态校验（script.Load，fail-fast）；
//  2. 能力探测：EVAL 必须，缺失返回 ErrCapability；
//  3. TODO(impl)：元数据恢复（meta 包）→ 挂载配置系统表（config 包）
//     → 启动后台角色循环（ReadWriteOnly 豁免，docs/08 §8.5）。
func NewKernel(cli Client, boot Bootstrap, opts ...Option) (*Kernel, error) {
	if cli == nil {
		return nil, errors.New("kidb: nil Client")
	}
	reg, err := script.Load()
	if err != nil {
		return nil, fmt.Errorf("kidb: lua asset load: %w", err)
	}
	k := &Kernel{
		cli:     cli,
		boot:    boot,
		logger:  slog.Default(),
		scripts: reg,
		closed:  make(chan struct{}),
	}
	for _, opt := range opts {
		opt(k)
	}
	if err := k.probeCapabilities(); err != nil {
		return nil, err
	}
	return k, nil
}

// probeCapabilities 执行启动期能力探测（docs/09 §9.4）：
// EVAL 缺失 → ErrCapability 拒绝启动；可选能力仅记录降级。
func (k *Kernel) probeCapabilities() error {
	probe, ok := k.scripts.Get("lock_release")
	if !ok {
		return fmt.Errorf("kidb: %w: probe script missing", ErrCapability)
	}
	// 用真实脚本做一次 EVAL 探针：token 不匹配时脚本返回 0，无副作用。
	_, err := k.cli.Eval(context.Background(), probe, []string{"lk:probe"}, "kidb-probe")
	if err != nil {
		return fmt.Errorf("kidb: %w: EVAL probe failed: %v", ErrCapability, err)
	}
	return nil
}

// Close 关闭内核：后台角色循环、janitor、连接池随此退出。
func (k *Kernel) Close() error {
	select {
	case <-k.closed:
	default:
		close(k.closed)
	}
	return k.closeErr
}

// Scripts 返回 Lua 资产注册表（测试与带外工具用）。
func (k *Kernel) Scripts() *script.Registry { return k.scripts }
