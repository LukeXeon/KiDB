// Package controller 是自治控制循环：锁选举（含 watchdog 闭环，
// docs/08 §8.5——移植 TiDB pkg/owner 语义）+ 桶分裂/合并决策。
//
// v1 落地：选举与故障迁移闭环（抢锁→watchdog 续约→失约立即退出→重新竞选）。
// 分裂/合并状态机（bucket_state_cas/split_migrate 协议，docs/08 §8.3）
// 与写路径双写联动是下一批。
package controller

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"kidb"
	"kidb/keycodec"
	"kidb/script"
	"kidb/utils"
)

// Elector 是 Redis 锁选举器。语义闭环（对齐 TiDB owner/manager.go）：
//
//	抢到锁 → 任职（启动角色循环）
//	在任期间 watchdog 每 TTL/3 续约
//	续约失败（锁丢/超时）→ 【立即】退出角色，停止一切控制动作
//	锁到期/主动退出 → 回竞选循环
type Elector struct {
	cli     kidb.KvClient
	reg     *script.Registry
	lockKey string
	token   string
	ttl     time.Duration
	isOwner atomic.Bool
}

// NewElector 构造。ttl 默认 10s（锁即选举，docs/08 §8.5）。
func NewElector(cli kidb.KvClient, reg *script.Registry, lockKey, token string, ttl time.Duration) *Elector {
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	return &Elector{
		cli: cli, reg: reg, lockKey: lockKey, token: token, ttl: ttl,
	}
}

// CtrlLock 返回全局控制锁选举器（lk:ctrl）。
func CtrlLock(cli kidb.KvClient, reg *script.Registry, instanceID string) *Elector {
	return NewElector(cli, reg, keycodec.CtrlLockKey(), instanceID, 0)
}

// IsOwner 报告当前是否在任。
func (e *Elector) IsOwner() bool { return e.isOwner.Load() }

// Campaign 竞选循环（阻塞；ctx 取消即退出）。role 在任职期间运行，
// 其 ctx 在失约/卸任时取消——角色必须响应取消（watchdog 闭环的关键动作）。
func (e *Elector) Campaign(ctx context.Context, role func(ctx context.Context) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ok, err := e.tryAcquire(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			// 瞬时错误退避后重竞选
			if !utils.SleepCtx(ctx, e.ttl/3) {
				return ctx.Err()
			}
			continue
		}
		if !ok {
			if !utils.SleepCtx(ctx, e.ttl/3+jitter()) {
				return ctx.Err()
			}
			continue
		}
		// 任职：角色与续约**并发**跑（角色阻塞在前台，watchdog 在后台盯续约；
		// 任一结束——角色退出或续约失败——都收掉另一方）
		e.isOwner.Store(true)
		roleCtx, cancel := context.WithCancel(ctx)
		watchDone := make(chan struct{})
		go func() {
			e.runWatchdog(roleCtx, cancel) // 续约失败 → cancel roleCtx
			close(watchDone)
		}()
		roleErr := runRole(roleCtx, role)
		cancel() // 角色退出 → 停续约循环
		<-watchDone
		e.isOwner.Store(false)
		e.release(context.Background()) // 卸任释放（锁在任期自然到期也可）
		_ = roleErr
		// 重新竞选
	}
}

// tryAcquire 抢锁（SET NX PX）。
func (e *Elector) tryAcquire(ctx context.Context) (bool, error) {
	res, err := e.cli.Do(ctx, "SET", e.lockKey, e.token, "PX", int(e.ttl.Milliseconds()), "NX")
	if err != nil {
		return false, err
	}
	return res != nil && fmt.Sprint(res) == "OK", nil
}

// runWatchdog 续约循环：续约失败【立即】cancel 角色 ctx（锁已丢，杜绝脑裂窗口）。
func (e *Elector) runWatchdog(roleCtx context.Context, cancel context.CancelFunc) error {
	renew, ok := e.reg.Get("lock_renew")
	if !ok {
		cancel()
		return fmt.Errorf("controller: lock_renew.lua not registered")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(e.ttl / 3)
		defer t.Stop()
		for {
			select {
			case <-roleCtx.Done():
				return
			case <-t.C:
				ctx, cancelT := context.WithTimeout(context.Background(), e.ttl/3)
				res, err := e.cli.Eval(ctx, renew, []string{e.lockKey}, e.token, fmt.Sprint(e.ttl.Milliseconds()))
				cancelT()
				if err != nil || fmt.Sprint(res) != "1" {
					cancel() // 续约失败 → 立即退出角色
					return
				}
			}
		}
	}()
	<-done
	return nil
}

// runRole 运行角色直到其退出或 ctx 取消。
func runRole(ctx context.Context, role func(ctx context.Context) error) error {
	done := make(chan error, 1)
	go func() { done <- role(ctx) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

// release 卸任释放锁（token 比对，lock_release.lua）。
func (e *Elector) release(ctx context.Context) {
	rel, ok := e.reg.Get("lock_release")
	if !ok {
		return
	}
	c, cancel := context.WithTimeout(ctx, e.ttl/3)
	defer cancel()
	_, _ = e.cli.Eval(c, rel, []string{e.lockKey}, e.token)
}

// 竞选循环经 ctx 取消停止。

func jitter() time.Duration {
	return time.Duration(time.Now().UnixNano()%100) * time.Millisecond // 轻量抖动
}
