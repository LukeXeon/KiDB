package keycodec

import (
	"fmt"
	"net/url"
	"strings"
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

// CntKey 行计数器：`cnt:{table}:{stag}`。
func CntKey(table string, slot uint16) string {
	return "cnt:" + table + ":" + SlotTag(slot)
}

// AsyncLogKey 异步索引日志：`log:idx:{table}:{idx}:{stag}`。
func AsyncLogKey(table, idx string, slot uint16) string {
	return "log:idx:" + table + ":" + idx + ":" + SlotTag(slot)
}

// BucketMapKey 桶路由表：`bm:{table}:{idx}`。
func BucketMapKey(table, idx string) string { return "bm:" + table + ":" + idx }

// CatalogKey 表元数据：`c:table:{table}`。
func CatalogKey(table string) string { return "c:table:" + table }

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

// CtrlLockKey Controller 选举锁：`lk:ctrl`。
func CtrlLockKey() string { return "lk:ctrl" }

// SweepLockKey Sweeper 区间锁：`lk:sweep:{slot区间}`。
func SweepLockKey(slotStart, slotEnd uint16) string {
	return fmt.Sprintf("lk:sweep:{%d-%d}", slotStart, slotEnd)
}

// EscapeValue 桶/预约 key 中 value 的转义规则（docs/03 §3.2）：
// URL escape；超长（>128B）或含 ':'、'{'、'}'、'#' 的值改取摘要
// （TODO(impl)：摘要变体随 cespare/xxhash 依赖落地，桶 key 带 "~x" 前缀）。
func EscapeValue(v string) string {
	return url.QueryEscape(v)
}

// HasDigestPrefix 报告桶 key 的 value 段是否为摘要形态。
func HasDigestPrefix(escaped string) bool {
	return strings.HasPrefix(escaped, "~x")
}
