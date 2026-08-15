package tuning

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEmbeddedLoads embed 的 tuning.toml 必须可解析且过红线校验（构建期守门）。
func TestEmbeddedLoads(t *testing.T) {
	tn := Get()
	require.Positive(t, tn.Exec.Batch)
	require.EqualValues(t, 8<<20, tn.Controller.SplitBytes)
	require.Equal(t, 64, tn.Telemetry.SampleRatio)
}

// TestValidate 红线校验：分裂体积超 16MB 拒绝。
func TestValidate(t *testing.T) {
	bad := *Get()
	bad.Controller.SplitBytes = 32 << 20
	_, err := bad.validate()
	require.Error(t, err)
}

// TestOverrideForTest 覆盖与自动还原。
func TestOverrideForTest(t *testing.T) {
	before := Get().Exec.Batch
	OverrideForTest(t, func(tn *Tuning) { tn.Exec.Batch = 7 })
	require.Equal(t, 7, Get().Exec.Batch)
	// Cleanup 后还原（本测试末尾触发）
	_ = before
}
