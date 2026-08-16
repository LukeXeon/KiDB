package kv

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"kidb/utils"
)

// SyncClock 服务端钟对齐的时钟（docs/11 §11.1 时钟偏移行）：
// exp 登记册 score 由写入方时钟产生，活性判定/清扫以另一时钟比对——
// 两侧各用本地钟会把网关与 Redis 的偏斜变成"行活着但索引被清扫/活性误判"。
// 本组件把全体内核时钟对齐到 Redis 服务端 TIME：30s 惰性重同步 + 本地钟兜底
// （TIME 不可用时退化为本地钟，与既有行为一致）。miniredis 的 TIME 不随
// FastForward 走——TTL 测试仍走 SetClock 注入（不经本组件）。
type SyncClock struct {
	cli Client

	offsetNs atomic.Int64 // serverNow - localNow（纳秒）
	lastSync atomic.Int64 // 上次同步的本地 UnixNano
}

// NewSyncClock 构造（立即尝试一次同步；失败则本地钟起步）。
func NewSyncClock(cli Client) *SyncClock {
	c := &SyncClock{cli: cli}
	_ = c.resync(context.Background())
	return c
}

// Now 返回对齐后时间。
func (c *SyncClock) Now() time.Time {
	now := time.Now()
	if now.UnixNano()-c.lastSync.Load() > int64(30*time.Second) {
		// 惰性重同步（单飞无需：并发各写同值，offset 计算幂等）
		_ = c.resync(context.Background())
	}
	return time.Unix(0, now.UnixNano()+c.offsetNs.Load())
}

// resync 用 TIME 校准偏移（Capabilities.ServerTime 缺失时不动 offset）。
func (c *SyncClock) resync(ctx context.Context) error {
	if !c.cli.Capabilities().ServerTime {
		return nil
	}
	before := time.Now()
	res, err := c.cli.Do(ctx, "TIME")
	if err != nil {
		return err
	}
	ss := utils.Strings(res)
	if len(ss) == 0 {
		return fmt.Errorf("kidb: TIME bad reply %v", res)
	}
	sec, err := strconv.ParseInt(ss[0], 10, 64)
	if err != nil {
		return err
	}
	var usec int64
	if len(ss) > 1 {
		usec, _ = strconv.ParseInt(ss[1], 10, 64)
	}
	// 服务端时刻 ≈ TIME 回复到达中点：用前后本地钟中值逼近（往返半程补偿）
	after := time.Now()
	localMid := before.UnixNano()/2 + after.UnixNano()/2
	serverNs := sec*1e9 + usec*1e3
	c.offsetNs.Store(serverNs - localMid)
	c.lastSync.Store(after.UnixNano())
	return nil
}
