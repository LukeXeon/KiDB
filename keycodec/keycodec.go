package keycodec

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// key 布局规范见 docs/03 §3.1（v7.0：桶按值/索引独立寻址，与行 slot 解绑）。
// 记号：{table} 为裸插值占位；{pk}/{encVal}/{r|l 子桶号}/{stag} 为字面
// 大括号包裹的 hash tag。行 slot 内聚收窄为"行 + 回执 + 异步日志"。

// RowKey 行数据 key：`d:{table}:{pk}`（tag 即 pk）。
func RowKey(table, pk string) string {
	return "d:" + table + ":{" + pk + "}"
}

// ReceiptKey 过期回执 key：`rcpt:{table}:{pk}`（与行同 slot）。
func ReceiptKey(table, pk string) string {
	return "rcpt:" + table + ":{" + pk + "}"
}

// ExpKey 过期登记册 key（集中单册形态）：`exp:{table}`。
func ExpKey(table string) string {
	return "exp:" + table
}

// ExpShardKey 细分登记册 key：`exp:{table}#{n}`（docs/07 §7.2——
// 无显式 tag，整 key 参与散列，不同 n 天然落不同 slot）。
func ExpShardKey(table string, shard int) string {
	return fmt.Sprintf("%s#%d", ExpKey(table), shard)
}

// ExpKeyN 登记册规范形态的唯一入口：shards≤1 时无后缀（与 ExpKey 一致），
// 否则带 `#shard` 后缀。写/扫/清扫三方必须都经此函数（docs/03 §3.1）。
func ExpKeyN(table string, shard, shards int) string {
	if shards <= 1 {
		return ExpKey(table)
	}
	return ExpShardKey(table, shard)
}

// ExpShardFor 按 pk 散列选登记册分片（docs/07 §7.2：按 pk 散列细分）。
func ExpShardFor(pk string, shards int) int {
	if shards <= 1 {
		return 0
	}
	return int(CRC16(pk)) % shards
}

// EqBucketKey 等值索引桶：`i:{table}:{idx}={encVal}`（默认单桶）。
// value 经 EscapeValue 转义/摘要（摘要规则恰好保证 tag 无结构字符，docs/03 §3.2）。
func EqBucketKey(table, idx, value string, n int) string {
	return EqBucketKeyEsc(table, idx, EscapeValue(value), n)
}

// EqBucketKeyEsc 以**已转义** value 段直接建桶 key（bm 注册表/回执/分裂协议
// 内部流通的都是转义形态——经 EqBucketKey 二次转义会错位（review 实证））。
// 子桶（n>0，分裂态）：tag = `encVal#b{n}`——`#b{n}` 编入 tag 使子桶摊异 slot
// （EscapeValue 已保证 encVal 不含 '#'，分隔符安全）。
func EqBucketKeyEsc(table, idx, encVal string, n int) string {
	if n <= 0 {
		return "i:" + table + ":" + idx + "={" + encVal + "}"
	}
	return fmt.Sprintf("i:%s:%s={%s#b%d}", table, idx, encVal, n)
}

// EqSubFor 等值子桶选择：写/删按 xxhash64(pk) % K 确定性选子桶
// （撤建同函数可寻，docs/03 §3.3）。
func EqSubFor(pk string, k int) int {
	if k <= 1 {
		return 0
	}
	return int(xxhash.Sum64String(pk) % uint64(k))
}

// RangeBucketKey 范围索引桶：`i:{table}:{idx}:{r子桶号}`（默认子桶号 0 单桶；
// 分裂后子桶各占异 slot，docs/03 §3.3）。
func RangeBucketKey(table, idx string, n int) string {
	return fmt.Sprintf("i:%s:%s:{r%d}", table, idx, n)
}

// LexBucketKey 字典序副本桶：`i:{table}:{idx}:{l子桶号}`。
func LexBucketKey(table, idx string, n int) string {
	return fmt.Sprintf("i:%s:%s:{l%d}", table, idx, n)
}

// ReplicaKey 热桶读副本：把源桶 key 的 hash tag 替换为异 slot 的 stag
// （docs/08 §8.4 L4：副本必须落在与源桶不同的 slot）。
// 步进 stride=1820（⌊16384/9⌋，最大 8 副本+1）：k∈[1,8] 的偏移
// k×1820 (mod 16384) 均不撞源 slot 且彼此分散。
// 桶 key 中第一个 {} 对即 hash tag（value 段经 EscapeValue 转义，不含字面大括号）。
func ReplicaKey(bucketKey string, k int) string {
	const stride = 16384 / 9
	i := strings.IndexByte(bucketKey, '{')
	j := strings.IndexByte(bucketKey[i:], '}')
	if i < 0 || j <= 0 {
		panic("keycodec: ReplicaKey on key without hash tag: " + bucketKey)
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

// AsyncLogKey 异步索引日志：`log:idx:{table}:{idx}:{stag}`
// （保持行 slot 形态：redo 与行写在同一个行 Lua 内原子落盘，docs/05 §5.2）。
func AsyncLogKey(table, idx string, slot uint16) string {
	return "log:idx:" + table + ":" + idx + ":" + SlotTag(slot)
}

// DLQKey 异步补写死信队列：`dlq:idx:{table}:{idx}`（v7.0 触发四③——
// 补写硬失败（桶被 DROP/日志畸形）条目的最终落点，docs/12 §12.8）。
func DLQKey(table, idx string) string { return "dlq:idx:" + table + ":" + idx }

// BucketMapKey 桶路由表文档：`bm:{table}:{idx}`（v7.0 集中每索引一文档——
// 16384 分片消除：桶不再按行 slot 散布，跨 slot CAS 物理冲突随之消失）。
func BucketMapKey(table, idx string) string {
	return "bm:" + table + ":" + idx
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

// SweepLockKey Sweeper 册锁：`lk:sweep:{table}[#{n}]`
// （v7.0：登记册集中后分工粒度从 slot 区间改为 表×分片，docs/07 §7.3）。
func SweepLockKey(table string, shard, shards int) string {
	if shards <= 1 {
		return "lk:sweep:{" + table + "}"
	}
	return fmt.Sprintf("lk:sweep:{%s#%d}", table, shard)
}

// EscapeValue 桶/预约 key 中 value 的转义规则（docs/03 §3.2）：
// URL escape；超长（>128B）或含 ':'、'{'、'}'、'#' 的值改取 xxhash64 摘要
// （桶 key 带 "~x" 前缀标记为摘要桶，查询侧同规则寻址——两径同源本函数）。
// 摘要规则恰好保证转义形态不含 tag 结构字符——等值桶 tag = `{encVal}` 零新机制。
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

// ParseRangeBucketKey 反解范围桶 key（`i:{table}:{idx}:{r{n}}`）：
// 遥测/Controller 从桶 key 还原定位信息用。
func ParseRangeBucketKey(key string) (table, idx string, sub int, ok bool) {
	if !strings.HasPrefix(key, "i:") {
		return "", "", 0, false
	}
	rest := key[2:]
	colon := strings.IndexByte(rest, ':')
	if colon < 0 {
		return "", "", 0, false
	}
	table = rest[:colon]
	rest = rest[colon+1:]
	tagPos := strings.IndexByte(rest, '{')
	if tagPos < 0 {
		return "", "", 0, false
	}
	idx = rest[:tagPos-1] // 去尾 ':'
	end := strings.IndexByte(rest, '}')
	if end < 0 {
		return "", "", 0, false
	}
	n, sok := parseSub("r", rest[tagPos+1:end])
	if !sok {
		return "", "", 0, false
	}
	return table, idx, n, true
}

// parseSub 反解 tag 内子桶号（`r{n}`/`l{n}`/`{encVal#b{n}}` 的数字段）。
// 形态只由 keycodec 自身生成；不符 = key 非法（与既有 malformed 分支同纪律）。
func parseSub(prefix, tagContent string) (int, bool) {
	if !strings.HasPrefix(tagContent, prefix) {
		return 0, false
	}
	n, err := strconv.Atoi(tagContent[len(prefix):])
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// ParseEqBucketKey 反解等值桶 key（`i:{table}:{idx}={encVal[#b{n}]}`）：
// 遥测/Controller 从桶 key 还原定位信息用（桶 key 只由 keycodec 生成，
// encVal 经 EscapeValue 转义，不含 `#b` 分隔符）。
func ParseEqBucketKey(key string) (table, idx, encVal string, sub int, ok bool) {
	if !strings.HasPrefix(key, "i:") {
		return "", "", "", 0, false
	}
	rest := key[2:]
	colon := strings.IndexByte(rest, ':')
	if colon < 0 {
		return "", "", "", 0, false
	}
	table = rest[:colon]
	rest = rest[colon+1:]
	eqPos := strings.IndexByte(rest, '=')
	tagPos := strings.IndexByte(rest, '{')
	if eqPos < 0 || tagPos < 0 || eqPos != tagPos-1 {
		return "", "", "", 0, false
	}
	idx = rest[:eqPos]
	end := strings.IndexByte(rest, '}')
	if end < 0 {
		return "", "", "", 0, false
	}
	tag := rest[tagPos+1 : end]
	if h := strings.Index(tag, "#b"); h >= 0 { // 子桶形态：tag = encVal#b{n}
		n, sok := parseSub("", tag[h+2:])
		if !sok {
			return "", "", "", 0, false
		}
		return table, idx, tag[:h], n, true
	}
	// encVal 经 EscapeValue 绝不产出 '#'——tag 中的 '#' 只能是 #b{n} 分隔符
	if strings.Contains(tag, "#") {
		return "", "", "", 0, false
	}
	return table, idx, tag, 0, true
}
