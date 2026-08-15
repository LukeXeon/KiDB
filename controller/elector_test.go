package controller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"kidb/internal/redistest"
	"kidb/keycodec"
)

// TestElectionFailover 选主与故障迁移（docs/08 §8.5）：
// 锁即选举、watchdog 续约、持有者宕机后秒级接管、续约失败立即卸任。
func TestElectionFailover(t *testing.T) {
	cli, reg, _ := redistest.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ttl := 300 * time.Millisecond
	e1 := NewElector(cli, reg, keycodec.CtrlLockKey(), "inst-1", ttl)
	e2 := NewElector(cli, reg, keycodec.CtrlLockKey(), "inst-2", ttl)

	var roleRuns1, roleRuns2 atomic.Int32
	role := func(counter *atomic.Int32) func(context.Context) error {
		return func(ctx context.Context) error {
			counter.Add(1)
			<-ctx.Done() // 角色响应取消（watchdog 语义）
			return ctx.Err()
		}
	}

	go e1.Campaign(ctx, role(&roleRuns1))
	require.Eventually(t, e1.IsOwner, 2*time.Second, 10*time.Millisecond, "inst-1 应抢到锁任职")
	// 关键：在任期间角色必须真的在跑（反死锁断言）
	require.Eventually(t, func() bool { return roleRuns1.Load() > 0 }, 2*time.Second, 10*time.Millisecond,
		"在任但角色未运行 = 死锁（watchdog/角色必须并发）")

	go e2.Campaign(ctx, role(&roleRuns2))
	time.Sleep(150 * time.Millisecond)
	require.False(t, e2.IsOwner(), "锁被持有时 inst-2 不得任职")

	// 持有者"宕机"（停止续约+取消竞选上下文）
	cancel()
	require.Eventually(t, func() bool { return !e1.IsOwner() }, 2*time.Second, 10*time.Millisecond)

	// 新持有者（新 ctx）
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	e3 := NewElector(cli, reg, keycodec.CtrlLockKey(), "inst-3", ttl)
	go e3.Campaign(ctx2, role(&roleRuns2))
	require.Eventually(t, e3.IsOwner, 2*time.Second, 10*time.Millisecond, "锁到期后 inst-3 应接管（断点续作的前提）")
}

// TestWatchdogStepDown 续约失败（锁被外部删除）→ 立即卸任（脑裂窗口消除）。
func TestWatchdogStepDown(t *testing.T) {
	cli, reg, _ := redistest.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ttl := 300 * time.Millisecond
	e := NewElector(cli, reg, keycodec.CtrlLockKey(), "inst-1", ttl)

	roleExited := make(chan struct{})
	go e.Campaign(ctx, func(ctx context.Context) error {
		defer close(roleExited)
		<-ctx.Done()
		return ctx.Err()
	})
	require.Eventually(t, e.IsOwner, 2*time.Second, 10*time.Millisecond)

	// 外部删锁 → 下次续约必失败 → watchdog 应立即 cancel 角色
	_, err := cli.Do(context.Background(), "DEL", keycodec.CtrlLockKey())
	require.NoError(t, err)
	select {
	case <-roleExited:
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog 未在续约失败后立即退出角色（docs/08 §8.5 脑裂窗口）")
	}
}
