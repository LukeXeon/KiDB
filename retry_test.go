package kidb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClassifyError(t *testing.T) {
	cases := map[string]ErrClass{
		"ERR Error compiling script: CROSSSLOT Keys in request": ClassFatal,
		"MOVED 1234 127.0.0.1:7001":      ClassRedirect,
		"ASK 1234 ...":                   ClassRedirect,
		"CLUSTERDOWN Hash slot not served": ClassClusterDown,
		"LOADING Redis is loading the dataset in memory": ClassLoading,
		"READONLY You can't write against a read only replica": ClassReadOnly,
		"TRYAGAIN Multiple keys request": ClassTryAgain,
		"dial tcp: i/o timeout":        ClassTransient,
		"context deadline exceeded":    ClassUnknown, // 内核语境不归类为可重试
	}
	for msg, want := range cases {
		if got := ClassifyError(errors.New(msg)); got != want {
			t.Errorf("ClassifyError(%q) = %v, want %v", msg, got, want)
		}
	}
}

func TestWithRetry(t *testing.T) {
	ctx := context.Background()
	pol := RetryPolicy{Base: time.Millisecond, Max: 5 * time.Millisecond, MaxAttempts: 3}

	// 瞬时错误重试到成功
	calls := 0
	err := WithRetry(ctx, pol, func() error {
		calls++
		if calls < 3 {
			return errors.New("TRYAGAIN later")
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, calls)

	// Fatal 不重试
	calls = 0
	err = WithRetry(ctx, pol, func() error {
		calls++
		return errors.New("CROSSSLOT keys")
	})
	require.Error(t, err)
	require.Equal(t, 1, calls, "CROSSSLOT 不重试")

	// CLUSTERDOWN 耗尽映射 1105
	err = WithRetry(ctx, pol, func() error { return errors.New("CLUSTERDOWN") })
	require.ErrorIs(t, err, ErrClusterUnavailable)

	// MOVED 耗尽映射
	err = WithRetry(ctx, pol, func() error { return errors.New("MOVED 1 x") })
	require.ErrorIs(t, err, ErrRedirectExhausted)

	// ctx 取消退出
	c2, cancel := context.WithCancel(ctx)
	cancel()
	err = WithRetry(c2, pol, func() error { return errors.New("TRYAGAIN") })
	require.ErrorIs(t, err, context.Canceled)
}
