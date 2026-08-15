package kidb

import (
	"context"
	"strings"
	"time"
)

// retry.go：错误分类与退避矩阵（docs/09 §9.6，移植 client-go Backoffer 思想）：
// 按错误类型分派退避策略与上限——不再"一律重试"。

// ErrClass 是故障类别。
type ErrClass int

const (
	ClassFatal     ErrClass = iota // 不重试（CROSSSLOT 等契约违例 = 内核 bug）
	ClassRedirect                  // MOVED/ASK（适配器已跟随；耗尽后整体可重试）
	ClassClusterDown               // CLUSTERDOWN：集群不可用，指数退避
	ClassLoading                   // LOADING：节点加载数据集，退避上限放宽
	ClassReadOnly                  // READONLY：failover 窗口，退避 + 拓扑刷新
	ClassTryAgain                  // TRYAGAIN：短退避
	ClassTransient                 // 超时/连接断开（读安全重试；写靠幂等）
	ClassUnknown
)

// ClassifyError 把 Redis 错误归类。
func ClassifyError(err error) ErrClass {
	if err == nil {
		return ClassUnknown
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "CROSSSLOT"), strings.Contains(msg, "WRONGTYPE"):
		return ClassFatal
	case strings.Contains(msg, "MOVED"), strings.Contains(msg, "ASK"):
		return ClassRedirect
	case strings.Contains(msg, "CLUSTERDOWN"):
		return ClassClusterDown
	case strings.Contains(msg, "LOADING"):
		return ClassLoading
	case strings.Contains(msg, "READONLY"):
		return ClassReadOnly
	case strings.Contains(msg, "TRYAGAIN"):
		return ClassTryAgain
	case strings.Contains(msg, "timeout"), strings.Contains(msg, "EOF"), strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "connection refused"), strings.Contains(msg, "i/o timeout"):
		return ClassTransient
	}
	return ClassUnknown
}

// RetryPolicy 退避参数（docs/10 §10.2：retry_backoff_base_ms/_max_ms）。
type RetryPolicy struct {
	Base time.Duration // 默认 50ms
	Max  time.Duration // 默认 2s
	// MaxAttempts 按类别：CLUSTERDOWN/LOADING 放宽（节点恢复需秒级）
	MaxAttempts int
}

// DefaultRetryPolicy 默认参数（docs/09 §9.6 表）。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{Base: 50 * time.Millisecond, Max: 2 * time.Second, MaxAttempts: 3}
}

// WithRetry 按类别退避重试 fn。ClassFatal/Unknown 不重试；
// ClassLoading 上限放宽（×4）；超时预算尊重 ctx。
func WithRetry(ctx context.Context, pol RetryPolicy, fn func() error) error {
	if pol.MaxAttempts <= 0 {
		pol.MaxAttempts = 3
	}
	var cls ErrClass
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		cls = ClassifyError(err)
		maxAttempts := pol.MaxAttempts
		if cls == ClassLoading || cls == ClassClusterDown {
			maxAttempts *= 4
		}
		switch cls {
		case ClassFatal, ClassUnknown:
			return err
		}
		if attempt >= maxAttempts-1 {
			switch cls {
			case ClassRedirect:
				return wrap(ErrRedirectExhausted, err)
			case ClassClusterDown, ClassLoading:
				return wrap(ErrClusterUnavailable, err)
			default:
				return err
			}
		}
		backoff := pol.Base << attempt
		if backoff > pol.Max {
			backoff = pol.Max
		}
		if !sleepRetryable(ctx, backoff) {
			return ctx.Err()
		}
	}
}

func wrap(sentinel, err error) error {
	return &retryError{sentinel: sentinel, err: err}
}

type retryError struct {
	sentinel error
	err      error
}

func (e *retryError) Error() string { return e.sentinel.Error() + ": " + e.err.Error() }
func (e *retryError) Unwrap() error { return e.err }

// Is 让 errors.Is(err, ErrRedirectExhausted/ErrClusterUnavailable) 生效。
func (e *retryError) Is(target error) bool { return target == e.sentinel }

func sleepRetryable(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
