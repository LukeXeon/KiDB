package controller

// elector_v7_test.go：v7.0 角色负载自适应（docs/08 §8.5）行为钉死：
// 忙闲退避竞选 / 任职退让 / 空窗上界强制竞选。

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"kidb/testutil"
)

// 忙节点退避、闲节点先拿锁（退避式竞选核心）。
func TestElectorBusyBackoff(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	idleCtx, idleStop := context.WithCancel(context.Background())
	defer idleStop()

	var busyFlag atomic.Int64
	busyFlag.Store(200) // 3 档退避
	busy := NewElector(cli, reg, "lk:test", "busy", 400*time.Millisecond)
	busy.SetBusyFunc(busyFlag.Load)
	idle := NewElector(cli, reg, "lk:test", "idle", 400*time.Millisecond)
	idle.SetBusyFunc(func() int64 { return 0 })

	role := func(ctx context.Context) error { <-ctx.Done(); return nil }
	go func() { _ = busy.Campaign(ctx, role) }()
	go func() { _ = idle.Campaign(idleCtx, role) }()

	require.Eventually(t, idle.IsOwner, 3*time.Second, 20*time.Millisecond, "闲节点必须先拿锁")
	require.False(t, busy.IsOwner(), "忙节点退避中不得拿锁")

	// 闲节点卸任（取消竞选即释放）后，闲下来的忙节点接管（退避 ≠ 永久豁免）
	busyFlag.Store(0)
	idleStop()
	require.Eventually(t, busy.IsOwner, 3*time.Second, 20*time.Millisecond, "前任卸任后原忙节点（已闲）应接管")
}

// 任职中变忙 → 主动卸任（role_concede_total{reason=busy}）。
func TestElectorConcedeWhenBusy(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var busyFlag atomic.Int64
	e := NewElector(cli, reg, "lk:test2", "x", 300*time.Millisecond)
	e.SetBusyFunc(busyFlag.Load)
	role := func(ctx context.Context) error { <-ctx.Done(); return nil }
	go func() { _ = e.Campaign(ctx, role) }()

	require.Eventually(t, e.IsOwner, 2*time.Second, 20*time.Millisecond, "应先拿锁")
	busyFlag.Store(200) // 变忙 ≥128 退让阈
	require.Eventually(t, func() bool { return !e.IsOwner() }, 3*time.Second, 20*time.Millisecond,
		"变忙后必须主动卸任")
}

// 空窗超上界：无视退避强制竞选（兜底"全员忙自治死透"）。
func TestElectorVacancyCeiling(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e := NewElector(cli, reg, "lk:test3", "x", 400*time.Millisecond)
	e.SetBusyFunc(func() int64 { return 200 }) // 3 档退避（~133ms 档长）
	e.SetMaxVacancy(150 * time.Millisecond)    // 测试上界
	e.backoffUnit = 50 * time.Millisecond      // 退避档长收窄（测试节奏）

	role := func(ctx context.Context) error { <-ctx.Done(); return nil }
	go func() { _ = e.Campaign(ctx, role) }()

	// 锁持续空 > 150ms 后，即使忙也必须强抢成功（退避 3 档 150ms 之外仍会拿到）
	require.Eventually(t, e.IsOwner, 3*time.Second, 20*time.Millisecond,
		"空窗超上界必须无视退避强制竞选")
}
