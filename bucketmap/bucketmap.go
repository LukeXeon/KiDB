// Package bucketmap 实现 BucketMap（桶路由表，docs/03 §3.1/§3.3）：
// 稀疏存储——默认 ACTIVE 单桶由规则推导不落盘，只有分裂/合并中间态与
// 分裂后的子桶布局持久化。
//
// v5.0 修订（实现期发现）：bm key 按 slot 分片（`bm:{table}:{idx}:{stag}`）——
// v4 文档的全局 `bm:{table}:{idx}` 与写 Lua 内版本 CAS 物理冲突（跨 slot
// 不可达，与 v4.2 唯一约束同类问题）。分片后写路径 CAS 单 slot 可达；
// 等值桶另有热值注册表（`bmh:{table}:{idx}`）避免读路径全 slot 加载分片。
package bucketmap

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/tinylib/msgp/msgp"

	"kidb"
	"kidb/ds"
	"kidb/keycodec"
	"kidb/script"
)

// State 桶状态机（docs/03 §3.3）。
type State string

const (
	Active     State = "ACTIVE"
	Splitting  State = "SPLITTING"
	Draining   State = "DRAINING"    // 分裂排水：写仅子桶、读仅子桶
	Merging    State = "MERGING"     // 合并双写：写目标+旧桶、读全集
	MergeDrain State = "MERGE_DRAIN" // 合并排水：写仅目标桶、读仅目标桶
	Deleted    State = "DELETED"
)

// SplitInfo 分裂/合并中间态（SPLITTING/DRAINING/MERGING）。
// 分裂：Parents=旧桶集，Children=新桶集（2×）；
// 合并：Parents=合并目标桶集（½），Children=被合并的旧桶集（镜像协议）。
type SplitInfo struct {
	State    State `json:"st"`
	Parents  []int `json:"p"`
	Children []int `json:"c"`
}

// EqEntry 等值桶条目（field `e:{encVal}`，稀疏）。
type EqEntry struct {
	Buckets []int      `json:"b"`           // ACTIVE 时的当前子桶下标集（默认 [0]）
	Split   *SplitInfo `json:"s,omitempty"` // 分裂中间态
}

// RangeBucket 范围桶（按 score 区间）。Children 为完整子桶记录
// （分裂点=采样中位数，须随状态持久化，写/读路径据此选边）。
type RangeBucket struct {
	Idx      int           `json:"i"`
	Lo       string        `json:"lo"` // "-inf" | 十进制浮点
	Hi       string        `json:"hi"` // "+inf" | ...
	State    State         `json:"st"`
	Children []RangeBucket `json:"c,omitempty"` // 分裂中间态时的两个子桶 [lo,mid) [mid,hi)
}

// Shard 是一个 (表, 索引, slot) 的 BucketMap 分片。
type Shard struct {
	Version uint64
	Next    int                 // 桶下标分配器
	Eq      map[string]*EqEntry // 等值/字典序副本条目（"l" 为字典序副本）
	Ranges  []RangeBucket       // 范围桶区间列表（默认单桶 [{0,-inf,+inf}]）
	loaded  bool
}

// DefaultShard 空分片（规则推导的默认形态）。
func DefaultShard() *Shard {
	return &Shard{
		Next:   1,
		Eq:     map[string]*EqEntry{},
		Ranges: []RangeBucket{{Idx: 0, Lo: "-inf", Hi: "+inf", State: Active}},
	}
}

//msgp:ignore Store

// Store 是 BucketMap 的读写存储（版本 CAS 经 bucket_state_cas.lua）。
type Store struct {
	cli kidb.KvClient
	reg *script.Registry

	mu    sync.RWMutex
	cache map[string]*Shard
	since time.Time
	ttl   time.Duration
}

// New 构造（缓存 TTL 1s——分裂中间态读侧容忍见 docs/08 §8.3）。
func New(cli kidb.KvClient, reg *script.Registry) *Store {
	return &Store{cli: cli, reg: reg, cache: map[string]*Shard{}, since: time.Now(), ttl: time.Second}
}

// Key 分片 key（keycodec 布局）。
func Key(table, idx string, slot uint16) string {
	return "bm:" + table + ":" + idx + ":" + keycodec.SlotTag(slot)
}

// RegistryKey 热值注册表（等值索引哪些值有分裂状态）。
func RegistryKey(table, idx string) string { return "bmh:" + table + ":" + idx }

// Load 读分片（短 TTL 缓存；强制刷新走 Invalidate）。
func (s *Store) Load(ctx context.Context, table, idx string, slot uint16) (*Shard, error) {
	k := Key(table, idx, slot)
	s.mu.RLock()
	if time.Since(s.since) < s.ttl {
		if sh, ok := s.cache[k]; ok {
			s.mu.RUnlock()
			return sh, nil
		}
	}
	s.mu.RUnlock()

	res, err := s.cli.Do(ctx, "HGETALL", k)
	if err != nil {
		return nil, err
	}
	sh := DefaultShard()
	fields, _ := ds.StringMap(res)
	if len(fields) > 0 {
		sh.Version = parseUint(fields["version"])
		sh.Next = int(parseUint(fields["next"]))
		if sh.Next == 0 {
			sh.Next = 1
		}
		for f, v := range fields {
			switch {
			case len(f) > 2 && f[:2] == "e:":
				var e EqEntry
				if _, err := e.UnmarshalMsg([]byte(v)); err == nil {
					sh.Eq[f[2:]] = &e
				}
			case f == "r":
				if rs, err := decodeRanges([]byte(v)); err == nil {
					sh.Ranges = rs
				}
			}
		}
	}
	s.mu.Lock()
	if time.Since(s.since) >= s.ttl {
		s.cache = map[string]*Shard{}
		s.since = time.Now()
	}
	s.cache[k] = sh
	s.mu.Unlock()
	return sh, nil
}

// LoadFresh 绕过缓存读分片（控制器 CAS 重试循环用）。
func (s *Store) LoadFresh(ctx context.Context, table, idx string, slot uint16) (*Shard, error) {
	s.Invalidate()
	return s.Load(ctx, table, idx, slot)
}

// Registry 读热值注册表（等值值名 → 是否有分裂状态；范围索引用哨兵 "@range"）。
// 读路径据此免加载全 slot 分片（docs/03 §3.1 稀疏原则）。
func (s *Store) Registry(ctx context.Context, table, idx string) (map[string]bool, error) {
	res, err := s.cli.Do(ctx, "HGETALL", RegistryKey(table, idx))
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	bmReply, _ := ds.StringMap(res)
	for f := range bmReply {
		out[f] = true
	}
	return out, nil
}

// RegisterHot 把值/哨兵写入注册表（分裂完成时调用）。
func (s *Store) RegisterHot(ctx context.Context, table, idx, field string) error {
	_, err := s.cli.Do(ctx, "HSET", RegistryKey(table, idx), field, 1)
	return err
}

// Invalidate 清缓存（写路径 stale / 控制器步进后调用）。
func (s *Store) Invalidate() {
	s.mu.Lock()
	s.cache = map[string]*Shard{}
	s.since = time.Now()
	s.mu.Unlock()
}

// CAS 原子步进一个字段（bucket_state_cas.lua）。值编码：条目类为 msgp
// 代码生成版（docs/03 §3.4），"next" 为十进制字符串。
func (s *Store) CAS(ctx context.Context, key string, expectVer uint64, field string, value any) (uint64, error) {
	raw, err := encodeBMValue(value)
	if err != nil {
		return 0, err
	}
	cs, _ := s.reg.Get("bucket_state_cas")
	out, err := s.cli.Eval(ctx, cs, []string{key}, strconv.FormatUint(expectVer, 10), field, string(raw))
	if err != nil {
		return 0, err
	}
	arr, _ := out.([]any)
	if len(arr) == 0 {
		return 0, fmt.Errorf("bucketmap: bad cas reply %v", out)
	}
	switch fmt.Sprint(arr[0]) {
	case "ok":
		return parseUint(fmt.Sprint(arr[1])), nil
	case "stale":
		s.Invalidate()
		return 0, fmt.Errorf("%w: bm %s expect %d got %s", kidb.ErrStaleMetadata, key, expectVer, fmt.Sprint(arr[1]))
	}
	return 0, fmt.Errorf("bucketmap: unknown cas status %v", arr[0])
}

// ==== 路由规则（写/读共用；docs/08 §8.3 状态规则的唯一实现落点）====

// WriteTargetsEq 等值桶写入目标（双写规则，docs/08 §8.3）：
// ACTIVE → 哈希取模单桶；SPLITTING → 父+子双写；DRAINING → 仅子桶；
// MERGING → 目标+旧桶双写；MERGE_DRAIN → 仅合并目标桶。
func (sh *Shard) WriteTargetsEq(encVal, pk string) []int {
	e := sh.Eq[encVal]
	if e == nil {
		return []int{0}
	}
	if e.Split != nil {
		parent := e.Split.Parents[subIdx(pk, len(e.Split.Parents))]
		child := e.Split.Children[subIdx(pk, len(e.Split.Children))]
		switch e.Split.State {
		case Splitting, Merging:
			return []int{parent, child}
		case Draining:
			return []int{child}
		case MergeDrain:
			return []int{parent}
		}
	}
	return []int{e.Buckets[subIdx(pk, len(e.Buckets))]}
}

// ReadBucketsEq 等值桶读取集合（搬迁窗口双读，成员去重在 exec）：
// SPLITTING/MERGING → 父+子全集；DRAINING → 子桶；MERGE_DRAIN → 目标桶。
func (sh *Shard) ReadBucketsEq(encVal string) []int {
	e := sh.Eq[encVal]
	if e == nil {
		return []int{0}
	}
	if e.Split != nil {
		switch e.Split.State {
		case Draining:
			return e.Split.Children
		case MergeDrain:
			return e.Split.Parents
		default: // Splitting / Merging
			return append(append([]int{}, e.Split.Parents...), e.Split.Children...)
		}
	}
	return e.Buckets
}

// WriteTargetsRange 范围桶写入目标（按 score 选区间；分裂选边按子桶区间边界）。
func (sh *Shard) WriteTargetsRange(score float64) []int {
	for _, rb := range sh.Ranges {
		if !rangeContains(rb, score) {
			continue
		}
		if len(rb.Children) == 2 {
			child := rb.Children[0]
			if rangeContains(rb.Children[1], score) {
				child = rb.Children[1]
			}
			switch rb.State {
			case Splitting:
				return []int{rb.Idx, child.Idx}
			case Merging:
				// 合并的 Children[0] 是目标桶（并集区间），Children[1] 是被并桶
				return []int{rb.Children[0].Idx, rb.Idx}
			case Draining:
				return []int{child.Idx}
			case MergeDrain:
				return []int{rb.Children[0].Idx}
			}
		}
		return []int{rb.Idx}
	}
	return []int{sh.Ranges[0].Idx} // 兜底（不应到达）
}

// ReadBucketsRange 范围谓词覆盖的桶集合。
func (sh *Shard) ReadBucketsRange(lo, hi float64) []int {
	var out []int
	for _, rb := range sh.Ranges {
		if !rangeOverlaps(rb, lo, hi) {
			continue
		}
		if len(rb.Children) == 2 {
			switch rb.State {
			case Draining:
				for _, c := range rb.Children {
					out = append(out, c.Idx)
				}
			case MergeDrain:
				out = append(out, rb.Children[0].Idx)
			default: // SPLITTING / MERGING：父+子双读
				out = append(out, rb.Idx)
				for _, c := range rb.Children {
					out = append(out, c.Idx)
				}
			}
		} else {
			out = append(out, rb.Idx)
		}
	}
	return out
}

// subIdx 等值子桶放置：xxhash64(pk) 高位取模（docs/03 §3.3）。
func subIdx(pk string, n int) int {
	return int(xxhash.Sum64String(pk) % uint64(n))
}

// rangeContains 区间 [lo,hi) 包含判定。
func rangeContains(rb RangeBucket, score float64) bool {
	lo, hi := parseBound(rb.Lo), parseBound(rb.Hi)
	return score >= lo && score < hi
}

// rangeOverlaps 区间与谓词区间重叠判定。
func rangeOverlaps(rb RangeBucket, lo, hi float64) bool {
	bLo, bHi := parseBound(rb.Lo), parseBound(rb.Hi)
	return bLo <= hi && lo <= bHi
}

// 范围桶区间工具见上方 rangeContains/rangeOverlaps。

func parseBound(s string) float64 {
	switch s {
	case "-inf":
		return math.Inf(-1)
	case "+inf":
		return math.Inf(1)
	}
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// FormatBound 边界编码。
func FormatBound(v float64) string {
	if math.IsInf(v, -1) {
		return "-inf"
	}
	if math.IsInf(v, 1) {
		return "+inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func parseUint(s string) uint64 {
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

// encodeBMValue bm 字段值编码（msgp 生成版；int 走十进制字符串）。
func encodeBMValue(v any) ([]byte, error) {
	switch t := v.(type) {
	case int:
		return []byte(strconv.Itoa(t)), nil
	case *EqEntry:
		return t.MarshalMsg(nil)
	case []RangeBucket:
		return encodeRanges(t)
	}
	return nil, fmt.Errorf("bucketmap: unsupported CAS value %T", v)
}

// encodeRanges 范围桶列表编码。
func encodeRanges(rs []RangeBucket) ([]byte, error) {
	b := msgp.AppendArrayHeader(nil, uint32(len(rs)))
	for i := range rs {
		var err error
		b, err = rs[i].MarshalMsg(b)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

// decodeRanges 范围桶列表解码。
func decodeRanges(b []byte) ([]RangeBucket, error) {
	sz, rest, err := msgp.ReadArrayHeaderBytes(b)
	if err != nil {
		return nil, err
	}
	out := make([]RangeBucket, 0, sz)
	for i := uint32(0); i < sz; i++ {
		var rb RangeBucket
		rest, err = rb.UnmarshalMsg(rest)
		if err != nil {
			return nil, err
		}
		out = append(out, rb)
	}
	return out, nil
}
