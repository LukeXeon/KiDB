package keycodec

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// key 布局规范见 docs/03 §3.1。记号：{table} 为裸插值占位；
// {pk}/{stag} 为字面大括号包裹的 hash tag。

// RowKey 行数据 key：`d:{table}:{pk}`（tag 即 pk）。
func RowKey(table, pk string) string {
	return "d:" + table + ":{" + pk + "}"
}

// ReceiptKey 过期回执 key：`rcpt:{table}:{pk}`（与行同 slot）。
func ReceiptKey(table, pk string) string {
	return "rcpt:" + table + ":{" + pk + "}"
}

// ExpKey 过期登记册 key（未细分形态）：`exp:{table}:{stag}`。
func ExpKey(table string, slot uint16) string {
	return "exp:" + table + ":" + SlotTag(slot)
}

// ExpShardKey 细分登记册 key：`exp:{table}:{stag}#{n}`（docs/07 §7.2）。
func ExpShardKey(table string, slot uint16, shard int) string {
	return fmt.Sprintf("%s#%d", ExpKey(table, slot), shard)
}

// ExpKeyN 登记册规范形态的唯一入口：shards≤1 时无后缀（与 ExpKey 一致），
// 否则带 `#shard` 后缀。写/扫/清扫三方必须都经此函数（docs/03 §3.1）。
func ExpKeyN(table string, slot uint16, shard, shards int) string {
	if shards <= 1 {
		return ExpKey(table, slot)
	}
	return ExpShardKey(table, slot, shard)
}

// ExpShardFor 按 pk 散列选登记册分片（docs/07 §7.2：按 pk 散列细分）。
func ExpShardFor(pk string, shards int) int {
	if shards <= 1 {
		return 0
	}
	return int(CRC16(pk)) % shards
}

// EqBucketKey 等值索引桶：`i:{table}:{idx}={value}:{stag}#b{n}`。
// value 经 EscapeValue 转义/摘要。
func EqBucketKey(table, idx, value string, slot uint16, n int) string {
	return fmt.Sprintf("i:%s:%s=%s:%s#b%d", table, idx, EscapeValue(value), SlotTag(slot), n)
}

// RangeBucketKey 范围索引桶：`i:{table}:{idx}:{stag}#r{n}`。
func RangeBucketKey(table, idx string, slot uint16, n int) string {
	return fmt.Sprintf("i:%s:%s:%s#r%d", table, idx, SlotTag(slot), n)
}

// LexBucketKey 字典序副本桶：`i:{table}:{idx}:{stag}#l{n}`。
func LexBucketKey(table, idx string, slot uint16, n int) string {
	return fmt.Sprintf("i:%s:%s:%s#l%d", table, idx, SlotTag(slot), n)
}

// ReplicaKey 热桶读副本：把源桶 key 的 stag 替换为异 slot 的 stag
// （docs/08 §8.4 L4：副本必须落在与源桶不同的 slot）。
// 步进 stride=1820（⌊16384/9⌋，最大 8 副本+1）：k∈[1,8] 的偏移
// k×1820 (mod 16384) 均不撞源 slot 且彼此分散。
// 桶 key 中唯一的大括号即 stag（value 段经 EscapeValue 转义，不含字面大括号）。
func ReplicaKey(bucketKey string, k int) string {
	const stride = 16384 / 9
	i := strings.IndexByte(bucketKey, '{')
	j := strings.IndexByte(bucketKey[i:], '}')
	if i < 0 || j <= 0 {
		panic("keycodec: ReplicaKey on key without stag: " + bucketKey)
	}
	newSlot := (Slot(bucketKey) + uint16(k*stride)) % NumSlots
	return bucketKey[:i] + SlotTag(newSlot) + bucketKey[i+j+1:]
}

// UniqueKey 唯一预约 key：`u:{table}:{idx}={encVal}`。
// key 只由值决定（同值必同 key 同 slot，SET NX 天然互斥，
// docs/05 §5.3）；不含任何 "{...}"，整个 key 参与散列。
func UniqueKey(table, idx, encVal string) string {
	return "u:" + table + ":" + idx + "=" + EscapeValue(encVal)
}

// AsyncLogKey 异步索引日志：`log:idx:{table}:{idx}:{stag}`。
func AsyncLogKey(table, idx string, slot uint16) string {
	return "log:idx:" + table + ":" + idx + ":" + SlotTag(slot)
}

// BucketMapSlotKey 桶路由表分片：`bm:{table}:{idx}:{stag}`（每 slot 分片——
// 全局单 key 与写 Lua 内版本 CAS 物理冲突，docs/03 §3.1）。
func BucketMapSlotKey(table, idx string, slot uint16) string {
	return "bm:" + table + ":" + idx + ":" + SlotTag(slot)
}

// BucketMapHotKey 热值注册表：`bmh:{table}:{idx}`（等值索引哪些值有分裂状态 +
// L4 副本登记，docs/03 §3.1）。
func BucketMapHotKey(table, idx string) string { return "bmh:" + table + ":" + idx }

// CatalogKey 表元数据：`c:table:{table}`。
func CatalogKey(table string) string { return "c:table:" + table }

// HLLKey 索引基数 HLL：`hll:{table}:{idx}`（索引级单 key——
// 消费方是索引级基数估算；per-slot 形态的 16384 路 PFCOUNT 扇出无消费方，见 docs/04 §4.6）。
func HLLKey(table, idx string) string { return "hll:" + table + ":" + idx }

// TableRegistryKey 表注册表：`c:tables` Hash（field=表名，value=1）。
// SHOW TABLES / INFORMATION_SCHEMA 由此生成（docs/02 §2.10）。
func TableRegistryKey() string { return "c:tables" }

// SchemaVerKey 全局 schema 版本：`ver:schema`（docs/06 §6.1）。
func SchemaVerKey() string { return "ver:schema" }

// CfgGlobalKey 全局配置：`cfg:global`。
func CfgGlobalKey() string { return "cfg:global" }

// VerKey 行版本计数器：`ver:{table}`（分片对策见 docs/08 §8.6）。
func VerKey(table string) string { return "ver:" + table }

// SeqKey 自增序列：`seq:{table}`。
func SeqKey(table string) string { return "seq:" + table }

// DropJobsKey DROP TABLE 清理作业注册表：`c:dropjobs`（Hash，field=表名）。
func DropJobsKey() string { return "c:dropjobs" }

// CtrlLockKey Controller 选举锁：`lk:ctrl`。
func CtrlLockKey() string { return "lk:ctrl" }

// SweepLockKey Sweeper 区间锁：`lk:sweep:{slot区间}`。
func SweepLockKey(slotStart, slotEnd uint16) string {
	return fmt.Sprintf("lk:sweep:{%d-%d}", slotStart, slotEnd)
}

// EscapeValue 桶/预约 key 中 value 的转义规则（docs/03 §3.2）：
// URL escape；超长（>128B）或含 ':'、'{'、'}'、'#' 的值改取 xxhash64 摘要
// （桶 key 带 "~x" 前缀标记为摘要桶，查询侧同规则寻址——两径同源本函数）。
func EscapeValue(v string) string {
	if len(v) > 128 || strings.ContainsAny(v, ":{}#") {
		return "~x" + strconv.FormatUint(xxhash.Sum64String(v), 16)
	}
	return url.QueryEscape(v)
}

// HasDigestPrefix 报告桶 key 的 value 段是否为摘要形态。
func HasDigestPrefix(escaped string) bool {
	return strings.HasPrefix(escaped, "~x")
}

// ParseRangeBucketKey 反解范围桶 key（`i:{table}:{idx}:{stag}#r{n}`）。
func ParseRangeBucketKey(key string) (table, idx string, slot uint16, sub int, ok bool) {
	if !strings.HasPrefix(key, "i:") {
		return "", "", 0, 0, false
	}
	rest := key[2:]
	colon := strings.IndexByte(rest, ':')
	if colon < 0 {
		return "", "", 0, 0, false
	}
	table = rest[:colon]
	rest = rest[colon+1:]
	tagPos := strings.IndexByte(rest, '{')
	if tagPos < 0 {
		return "", "", 0, 0, false
	}
	idx = rest[:tagPos-1] // 去尾 ':'
	end := strings.IndexByte(rest, '}')
	if end < 0 {
		return "", "", 0, 0, false
	}
	slot = Slot(rest[tagPos : end+1])
	sub = 0
	if h := strings.Index(rest[end:], "#r"); h >= 0 {
		fmt.Sscanf(rest[end+h:], "#r%d", &sub)
	}
	return table, idx, slot, sub, true
}

// ParseEqBucketKey 反解等值桶 key（`i:{table}:{idx}={value}:{stag}#b{n}`）：
// 遥测/Controller 从桶 key 还原定位信息用（桶 key 只由 keycodec 生成，
// value 段经 EscapeValue 转义，不含结构分隔符）。
func ParseEqBucketKey(key string) (table, idx, encVal string, slot uint16, sub int, ok bool) {
	if !strings.HasPrefix(key, "i:") {
		return "", "", "", 0, 0, false
	}
	rest := key[2:]
	colon := strings.IndexByte(rest, ':')
	if colon < 0 {
		return "", "", "", 0, 0, false
	}
	table = rest[:colon]
	rest = rest[colon+1:]
	eqPos := strings.IndexByte(rest, '=')
	tagPos := strings.IndexByte(rest, '{')
	if eqPos < 0 || tagPos < 0 || eqPos > tagPos {
		return "", "", "", 0, 0, false
	}
	idx = rest[:eqPos]
	encVal = rest[eqPos+1 : tagPos-1] // tag 前一字是 ':' 分隔符（value 内 ':' 已被转义）
	end := strings.IndexByte(rest, '}')
	if end < 0 {
		return "", "", "", 0, 0, false
	}
	tag := rest[tagPos : end+1]
	slot = Slot(tag)
	sub = 0
	if h := strings.Index(rest[end:], "#b"); h >= 0 {
		fmt.Sscanf(rest[end+h:], "#b%d", &sub)
	}
	return table, idx, encVal, slot, sub, true
}
