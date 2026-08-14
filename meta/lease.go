package meta

import (
	"sync"
	"time"
)

// LeaseTracker 实现 schema lease 纪律（docs/06 §6.2，移植 TiDB
// domain/SchemaValidator 的语义并按缓存定位放宽）：
//   - 租约窗口内信任本地快照（热路径零额外 RTT）；
//   - 越界必检：下一次元数据使用前必须比对 ver:schema；
//   - 正确性从不依赖 lease 守约（写路径 Lua 内 CAS 兜底）——lease 只是性能优化。
type LeaseTracker struct {
	Duration time.Duration // schema_lease_ms，默认 1s

	mu        sync.Mutex
	lastCheck time.Time
	lastVer   uint64
	valid     bool
}

// NewLeaseTracker 构造。
func NewLeaseTracker(d time.Duration) *LeaseTracker { return &LeaseTracker{Duration: d} }

// Fresh 报告本地快照是否仍在租约内（可直接信任，无需校验）。
func (l *LeaseTracker) Fresh(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.valid && now.Sub(l.lastCheck) < l.Duration
}

// Checked 记录一次成功校验：now 时刻比对远端版本 remoteVer；
// 返回远端是否相对本地快照发生了变化（true = 调用方应刷新快照）。
func (l *LeaseTracker) Checked(now time.Time, remoteVer uint64) (changed bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	changed = !l.valid || remoteVer != l.lastVer
	l.lastCheck = now
	l.lastVer = remoteVer
	l.valid = true
	return changed
}

// Invalidate 作废本地快照（如写路径收到 stale 后强制下次校验）。
func (l *LeaseTracker) Invalidate() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.valid = false
}
