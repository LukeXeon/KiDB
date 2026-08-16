package utils

import (
	"context"
	"time"
)

// SleepCtx 可取消睡眠（返回 false = ctx 已取消）。轮询/退避循环的统一形态。
func SleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
