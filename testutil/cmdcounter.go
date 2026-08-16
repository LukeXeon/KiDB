package testutil

import (
	"context"
	"strings"
	"sync"

	"kidb"
)

// cmdcounter.go：KvClient 命令计数包装（断言"覆盖路径零回表"/"投影走 HMGET"
// 等命令级证据的共用基建——此前 exec 与 gateway 各有一份近亲拷贝）。

// CmdCounter 统计内核发出的 Redis 命令；行 key（d: 前缀）的 HGETALL/HMGET
// 单独计数（回表证据——元数据 HGETALL 不算）。
type CmdCounter struct {
	kidb.KvClient
	mu         sync.Mutex
	counts     map[string]int
	rowHGETALL int
	rowHMGET   int
}

// NewCmdCounter 包装。
func NewCmdCounter(inner kidb.KvClient) *CmdCounter {
	return &CmdCounter{KvClient: inner, counts: map[string]int{}}
}

// Do 计数并透传。
func (c *CmdCounter) Do(ctx context.Context, cmd string, args ...any) (any, error) {
	c.mu.Lock()
	c.counts[cmd]++
	c.trackFetch(cmd, args)
	c.mu.Unlock()
	return c.KvClient.Do(ctx, cmd, args...)
}

// Pipeline 逐命令计数并透传。
func (c *CmdCounter) Pipeline(ctx context.Context, cmds []kidb.Cmd) ([]any, error) {
	c.mu.Lock()
	for _, cmd := range cmds {
		c.counts[cmd.Name]++
		c.trackFetch(cmd.Name, cmd.Args)
	}
	c.mu.Unlock()
	return c.KvClient.Pipeline(ctx, cmds)
}

func (c *CmdCounter) trackFetch(cmd string, args []any) {
	if len(args) == 0 {
		return
	}
	k, ok := args[0].(string)
	if !ok || !strings.HasPrefix(k, "d:") {
		return
	}
	switch cmd {
	case "HGETALL":
		c.rowHGETALL++
	case "HMGET":
		c.rowHMGET++
	}
}

// Count 该命令的累计次数。
func (c *CmdCounter) Count(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[name]
}

// RowFetches 行 key 的 HGETALL/HMGET 累计次数（回表证据）。
func (c *CmdCounter) RowFetches() (hgetall, hmget int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rowHGETALL, c.rowHMGET
}

// Reset 清零。
func (c *CmdCounter) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts = map[string]int{}
	c.rowHGETALL, c.rowHMGET = 0, 0
}
