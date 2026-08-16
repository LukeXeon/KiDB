package kidb

import (
	"context"

	"kidb/script"
)

// retry_client.go：退避矩阵（docs/09 §9.6）的装配落点——把 WithRetry 挂到
// KvClient 契约面。全部命令出口统一在此分派，消费方零感知。
//
// 为什么装饰器而不是逐调用点接线：重试策略是横切关注点，逐点接线必然产生
// 装配缺口（WithRetry 曾只在测试里活着——生产路径零重试）。
//
// 写路径重试安全性由幂等纪律支撑（write_row.lua 幂等，PBT P3 不变式断言
// "重复应用同写入不产生变化"）；读路径天然安全。pipeline 是逻辑批
// （无 MULTI），整批重试语义 = 逐命令重试。

// NewRetryingClient 包装 KvClient 为退避重试形态。
func NewRetryingClient(inner KvClient, pol RetryPolicy) KvClient {
	return &retryingClient{inner: inner, pol: pol}
}

type retryingClient struct {
	inner KvClient
	pol   RetryPolicy
}

func (c *retryingClient) Do(ctx context.Context, cmd string, args ...any) (any, error) {
	var out any
	err := WithRetry(ctx, c.pol, func() error {
		var err error
		out, err = c.inner.Do(ctx, cmd, args...)
		return err
	})
	return out, err
}

func (c *retryingClient) Pipeline(ctx context.Context, cmds []Cmd) ([]any, error) {
	var out []any
	err := WithRetry(ctx, c.pol, func() error {
		var err error
		out, err = c.inner.Pipeline(ctx, cmds)
		return err
	})
	return out, err
}

func (c *retryingClient) Eval(ctx context.Context, s *script.Script, keys []string, args ...any) (any, error) {
	var out any
	err := WithRetry(ctx, c.pol, func() error {
		var err error
		out, err = c.inner.Eval(ctx, s, keys, args...)
		return err
	})
	return out, err
}

func (c *retryingClient) DoReplica(ctx context.Context, cmd string, args ...any) (any, error) {
	var out any
	err := WithRetry(ctx, c.pol, func() error {
		var err error
		out, err = c.inner.DoReplica(ctx, cmd, args...)
		return err
	})
	return out, err
}

func (c *retryingClient) PipelineReplica(ctx context.Context, cmds []Cmd) ([]any, error) {
	var out []any
	err := WithRetry(ctx, c.pol, func() error {
		var err error
		out, err = c.inner.PipelineReplica(ctx, cmds)
		return err
	})
	return out, err
}

func (c *retryingClient) Capabilities() Capabilities { return c.inner.Capabilities() }

var _ KvClient = (*retryingClient)(nil)
