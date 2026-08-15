// Package telemetry 实现采样统计（docs/08 §8.1 信号源 A）：
// 读写路径 1/64 概率采样，命中时在同 slot 统计 key `st:{桶}` 累加 ops；
// 命中即登记候选（`hotcand` Hash），Controller 周期复核（ZCARD/MEMORY USAGE
// 精确值——阈值判断只信精确值，采样只负责发现）。
package telemetry

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"

	"kidb"
)

// CandKey 热桶候选注册表（Hash：field=桶 key，value=最近采样时间戳）。
const CandKey = "hotcand"

// Recorder 是采样记录器。
type Recorder struct {
	cli   kidb.KvClient
	ratio int // 采样率 1/ratio（默认 64，telemetry_sample_ratio）
	rnd   *rand.Rand
}

// New 构造。
func New(cli kidb.KvClient) *Recorder {
	return &Recorder{cli: cli, ratio: 64, rnd: rand.New(rand.NewSource(rand.Int63()))}
}

// SetRatio 覆盖采样率（配置热更新挂点）。
func (r *Recorder) SetRatio(n int) {
	if n > 0 {
		r.ratio = n
	}
}

// StatKey 桶统计 key（docs/03 §3.1：短期 TTL 滚动）。
func StatKey(bucketKey string) string { return "st:" + bucketKey }

// Sample 采样命中时：st:{桶} ops+1（短期 TTL）+ 登记候选。
// 非命中零成本（一次整数比较）。
func (r *Recorder) Sample(ctx context.Context, bucketKey string) {
	if r.rnd.Intn(r.ratio) != 0 {
		return
	}
	st := StatKey(bucketKey)
	_, _ = r.cli.Do(ctx, "HINCRBY", st, "ops", 1)
	_, _ = r.cli.Do(ctx, "PEXPIRE", st, 60000)
	_, _ = r.cli.Do(ctx, "HSET", CandKey, bucketKey, 0)
}

// Candidates 取候选桶列表（Controller 复核入口）。
func Candidates(ctx context.Context, cli kidb.KvClient) ([]string, error) {
	res, err := cli.Do(ctx, "HKEYS", CandKey)
	if err != nil {
		return nil, err
	}
	var out []string
	switch v := res.(type) {
	case []string:
		out = v
	case []any:
		for _, e := range v {
			out = append(out, fmt.Sprint(e))
		}
	}
	return out, nil
}

// Confirm 精确复核：返回桶成员数（ZCARD）并摘除候选。
func Confirm(ctx context.Context, cli kidb.KvClient, bucketKey string) (int64, error) {
	res, err := cli.Do(ctx, "ZCARD", bucketKey)
	if err != nil {
		return 0, err
	}
	n, _ := strconv.ParseInt(fmt.Sprint(res), 10, 64)
	_, _ = cli.Do(ctx, "HDEL", CandKey, bucketKey)
	return n, nil
}
