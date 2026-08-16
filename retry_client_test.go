package kidb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"kidb/script"
)

// flakyClient 前 fails 次以指定错误失败的假客户端。
type flakyClient struct {
	fails int32
	err   error
	calls int32
}

func (f *flakyClient) Do(ctx context.Context, cmd string, args ...any) (any, error) {
	atomic.AddInt32(&f.calls, 1)
	if atomic.AddInt32(&f.fails, -1) >= 0 {
		return nil, f.err
	}
	return "ok", nil
}
func (f *flakyClient) Pipeline(ctx context.Context, cmds []Cmd) ([]any, error) {
	out, err := f.Do(ctx, "", nil)
	return []any{out}, err
}
func (f *flakyClient) Eval(ctx context.Context, s *script.Script, keys []string, args ...any) (any, error) {
	return f.Do(ctx, "", nil)
}
func (f *flakyClient) DoReplica(ctx context.Context, cmd string, args ...any) (any, error) {
	return f.Do(ctx, cmd, args...)
}
func (f *flakyClient) PipelineReplica(ctx context.Context, cmds []Cmd) ([]any, error) {
	return f.Pipeline(ctx, cmds)
}
func (f *flakyClient) Capabilities() Capabilities { return Capabilities{} }

// TestRetryingClient 装饰器把退避矩阵接到契约面：
// TRANSIENT 类重试至成功；FATAL 类立即失败不重试；耗尽映射哨兵错误。
func TestRetryingClient(t *testing.T) {
	pol := RetryPolicy{Base: time.Millisecond, Max: 5 * time.Millisecond, MaxAttempts: 5}
	ctx := context.Background()

	// TRANSIENT（timeout）：重试 2 次后成功，底层调用 3 次
	f := &flakyClient{fails: 2, err: errors.New("i/o timeout")}
	cli := NewRetryingClient(f, pol)
	out, err := cli.Do(ctx, "GET", "k")
	require.NoError(t, err)
	require.Equal(t, "ok", out)
	require.Equal(t, int32(3), f.calls)

	// FATAL（CROSSSLOT = 契约违例）：不重试
	f2 := &flakyClient{fails: 10, err: errors.New("CROSSSLOT")}
	_, err = NewRetryingClient(f2, pol).Do(ctx, "GET", "k")
	require.Error(t, err)
	require.Equal(t, int32(1), f2.calls)

	// CLUSTERDOWN 耗尽 → ErrClusterUnavailable
	f3 := &flakyClient{fails: 100, err: errors.New("CLUSTERDOWN HASH")}
	_, err = NewRetryingClient(f3, pol).Do(ctx, "GET", "k")
	require.ErrorIs(t, err, ErrClusterUnavailable)

	// MOVED 耗尽 → ErrRedirectExhausted
	f4 := &flakyClient{fails: 100, err: errors.New("MOVED 1 127.0.0.1:6379")}
	_, err = NewRetryingClient(f4, pol).Do(ctx, "GET", "k")
	require.ErrorIs(t, err, ErrRedirectExhausted)

	// Pipeline 同权（整批重试）
	f5 := &flakyClient{fails: 1, err: errors.New("TRYAGAIN")}
	out5, err := NewRetryingClient(f5, pol).Pipeline(ctx, []Cmd{{Name: "GET", Args: []any{"k"}}})
	require.NoError(t, err)
	require.Equal(t, []any{"ok"}, out5)
}
