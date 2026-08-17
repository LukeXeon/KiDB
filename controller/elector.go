// Package controller 是自治控制循环：锁选举（含 watchdog 闭环，
// docs/08 §8.5——移植 TiDB pkg/owner 语义）+ 桶分裂/合并决策。
//
// v7.0：忙闲退避式竞选（触发二/四）——竞选前按本实例 inflight 分档退避
// （忙节点自然让闲节点先拿锁）；任职中变忙主动卸任；锁空窗超内置上界
// （300s，不设开关）所有节点无视退避强制竞选（兜底"全员忙自治死透"）。
package controller

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"kidb/keycodec"
	"kidb/kv"
	"kidb/metrics"
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
	cli     kv.Client
	reg     *script.Registry
	lockKey string
	token   string
	ttl     time.Duration
	isOwner atomic.Bool
	m       *metrics.Metrics // 指标（nil = no-op；owner_role_transitions_total）

	busy        func() int64  // 忙闲信号（本实例在跑查询数；nil = 恒 0，docs/08 §8.5）
	maxVacancy  time.Duration // 空窗上界（默认 300s；超时无视退避强制竞选）
	firstEmpty  atomic.Int64  // 连续空窗起点（unix 纳秒；0 = 锁有人在）
	backoffUnit time.Duration // 退避档长（默认 ttl/3；测试可注入）
}

// NewElector 构造。ttl 默认 10s（锁即选举，docs/08 §8.5）。
func NewElector(cli kv.Client, reg *script.Registry, lockKey, token string, ttl time.Duration) *Elector {
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	return &Elector{
		cli: cli, reg: reg, lockKey: lockKey, token: token, ttl: ttl,
		maxVacancy: 300 * time.Second,
	}
}

// SetMetrics 接入指标。
func (e *Elector) SetMetrics(m *metrics.Metrics) { e.m = m }

// SetBusyFunc 注入忙闲信号（DI 单点：在跑查询数，docs/08 §8.5 角色负载自适应）。
func (e *Elector) SetBusyFunc(f func() int64) { e.busy = f }

// SetMaxVacancy 覆盖空窗上界（测试注入小值；生产恒 300s 内置，不设开关）。
func (e *Elector) SetMaxVacancy(d time.Duration) { e.maxVacancy = d }

// backoff 竞选退避：inflight 每 64 个一档（上限 3 档），每档一个 ttl/3——
// 连续谱无阈值开关（v7.0 触发二纪律）；空窗超上界 → 0（无视退避强制竞选）。
func (e *Elector) backoff() time.Duration {
	if e.vacancy() > e.maxVacancy {
		return 0
	}
	if e.busy == nil {
		return 0
	}
	tier := e.busy() / 64
	if tier > 3 {
		tier = 3
	}
	unit := e.backoffUnit
	if unit <= 0 {
		unit = e.ttl / 3
	}
	return time.Duration(tier) * unit
}

// vacancy 当前连续空窗时长（0 = 锁有人在）。
func (e *Elector) vacancy() time.Duration {
	fe := e.firstEmpty.Load()
	if fe == 0 {
		return 0
	}
	return time.Since(time.Unix(0, fe))
}

// CtrlLock 返回全局控制锁选举器（lk:ctrl）。
func CtrlLock(cli kv.Client, reg *script.Registry, instanceID string) *Elector {
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
		// 忙闲退避（v7.0：忙节点让闲节点先拿锁；空窗超上界者豁免退避）
		if bo := e.backoff(); bo > 0 {
			if !utils.SleepCtx(ctx, bo+jitter()) {
				return ctx.Err()
			}
		}
		ok, err := e.tryAcquire(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			// 瞬时错误退避后重竞选
			if !utils.SleepCtx(ctx, e.ttl/3) {
				return ctx.Err()
			}
			continue
		}
		// 空窗追踪（抢锁失败说明锁在或竞争失败——GET 分辨）
		e.trackVacancy(ctx, ok)
		if !ok {
			if !utils.SleepCtx(ctx, e.ttl/3+jitter()) {
				return ctx.Err()
			}
			continue
		}
		// 任职：角色与续约**并发**跑（角色阻塞在前台，watchdog 在后台盯续约；
		// 任一结束——角色退出或续约失败——都收掉另一方）
		e.isOwner.Store(true)
		if e.m != nil {
			e.m.OwnerTransition.Inc()
		}
		roleCtx, cancel := context.WithCancel(ctx)
		watchDone := make(chan struct{})
		go func() {
			_ = e.runWatchdog(roleCtx, cancel) // 续约失败 → cancel roleCtx（返回即结束，错误无消费方）
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

// trackVacancy 空窗追踪：抢到/锁存在 → 清零；锁不存在（竞争失败）→ 累计。
// 空窗时长写指标（role_vacancy_seconds——"注册即接线"纪律）。
func (e *Elector) trackVacancy(ctx context.Context, acquired bool) {
	if acquired {
		e.firstEmpty.Store(0)
		if e.m != nil {
			e.m.RoleVacancy.Set(0)
		}
		return
	}
	res, err := e.cli.Do(ctx, "GET", e.lockKey)
	if err == nil && res == nil { // 锁不存在：空窗
		if e.firstEmpty.Load() == 0 {
			e.firstEmpty.Store(time.Now().UnixNano())
		}
	} else {
		e.firstEmpty.Store(0)
	}
	if e.m != nil {
		e.m.RoleVacancy.Set(e.vacancy().Seconds())
	}
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
				// 任职退让（v7.0 触发二）：在跑查询超阈（128 内置）→ 主动卸任
				// cancel 角色，锁待到期后由闲节点接走（role_concede_total{reason=busy}）
				if e.busy != nil && e.busy() >= 128 {
					if e.m != nil {
						e.m.RoleConcede.WithLabelValues("busy").Inc()
					}
					cancel()
					return
				}
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
