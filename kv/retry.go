package kv

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"strings"
	"time"

	"kidb/tuning"
	"kidb/utils"
)

// retry.go：错误分类与退避矩阵（docs/09 §9.6，移植 client-go Backoffer 思想）：
// 按错误类型分派退避策略与上限——不再"一律重试"。

// 耗尽哨兵（WithRetry 产出方即 owner；MySQL 错误码映射在根包 errors.go）。
var (
	ErrRedirectExhausted  = errors.New("ERR_REDIRECT_EXHAUSTED")  // MOVED/ASK 耗尽 → 1105
	ErrClusterUnavailable = errors.New("ERR_CLUSTER_UNAVAILABLE") // CLUSTERDOWN/LOADING 耗尽 → 1105
)

// ErrClass 是故障类别。
type ErrClass int

const (
	ClassFatal       ErrClass = iota // 不重试（CROSSSLOT 等契约违例 = 内核 bug）
	ClassRedirect                    // MOVED/ASK（适配器已跟随；耗尽后整体可重试）
	ClassClusterDown                 // CLUSTERDOWN：集群不可用，指数退避
	ClassLoading                     // LOADING：节点加载数据集，退避上限放宽
	ClassReadOnly                    // READONLY：failover 窗口，退避 + 拓扑刷新
	ClassTryAgain                    // TRYAGAIN：短退避
	ClassTransient                   // 超时/连接断开（读安全重试；写靠幂等）
	ClassUnknown
)

// ClassifyError 把 Redis 错误归类。
// 类型化判定优先（字符串嗅探对值内容误报脆弱——错误消息里带 "MOVED" 的
// 业务值不该被归类为重定向）；字符串兜底适配器/老版本 go-redis 的裸文本错误。
func ClassifyError(err error) ErrClass {
	if err == nil {
		return ClassUnknown
	}
	// 类型化通道：网络/ctx 错误
	var netErr net.Error
	if errors.As(err, &netErr) {
		return ClassTransient // 超时/断连（含 i/o timeout）
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return ClassTransient
	}
	if errors.Is(err, context.Canceled) {
		return ClassUnknown // 调用方取消不重试
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

// DefaultRetryPolicy 默认参数（docs/09 §9.6 表；数值在 tuning.toml [retry]）。
func DefaultRetryPolicy() RetryPolicy {
	tn := tuning.Get()
	return RetryPolicy{Base: time.Duration(tn.Retry.BackoffBaseMs) * time.Millisecond,
		Max: time.Duration(tn.Retry.BackoffMaxMs) * time.Millisecond, MaxAttempts: tn.Retry.MaxAttempts}
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
		// ±25% 抖动（CLUSTERDOWN 全集群同时退避的惊群效应，client-go 同款）
		backoff = backoff*3/4 + time.Duration(rand.Int63n(int64(backoff/2)+1))
		if !utils.SleepCtx(ctx, backoff) {
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
