// Package keycodec 是 KiDB 全部 key 布局的唯一所有者
// （docs/03 §3.1：对齐 TiDB tablecodec 纪律——
// 全仓库任何组件不得手工拼接 key 字符串）。
package keycodec

import (
	"fmt"
	"strings"

	"github.com/sigurn/crc16"
)

// NumSlots 是 Redis Cluster 的 slot 总数。
const NumSlots = 16384

// crc16Table 是 Redis Cluster 规范的 CRC16-XMODEM
// （poly 0x1021、init 0、非反射、无尾异或；docs/10 §10.1：
// 现成库 sigurn/crc16，多项式参数化实现，非硬编码表）。
var crc16Table = crc16.MakeTable(crc16.CRC16_XMODEM)

// CRC16 返回 Redis Cluster 规范的键哈希。
func CRC16(s string) uint16 {
	return crc16.Checksum([]byte(s), crc16Table)
}

// hashtag 提取 Redis Cluster hash tag：首个非空 "{...}" 的内容；
// 无 "{"、"{}" 为空（"{}" 紧邻）或无配对 "}" 时，整个 key 参与散列
// （Redis Cluster 规范原文语义）。
func hashtag(key string) string {
	i := strings.IndexByte(key, '{')
	if i < 0 {
		return key
	}
	rest := key[i+1:]
	j := strings.IndexByte(rest, '}')
	if j <= 0 { // 无配对 "}"，或 "{}" 为空（j==0）
		return key
	}
	return rest[:j]
}

// Slot 返回 key 的 slot：CRC16(hashtag) % 16384（契约 R1，
// docs/09 §9.3：适配器路由必须与本函数一致，由一致性测试套件强制校验）。
func Slot(key string) uint16 {
	return CRC16(hashtag(key)) % NumSlots
}

// stagTable 是离线预生成的 slot tag 表：stagTable[i] 满足
// Slot(stagTable[i]) == i（docs/03 §3.2）。init 一次性算出。
var stagTable [NumSlots]string

func init() {
	// 单趟候选扫描（coupon collector）：每个候选 tag 分给首个未占的 slot，
	// 先到先得不重算——O(N log N)，启动期一次。
	// 逐 slot 定向扫描是 O(N²)（末位 slot 平均要扫上万候选），不可用于启动路径。
	filled := 0
	for n := 0; filled < NumSlots; n++ {
		cand := fmt.Sprintf("{s:%05d}", n) // %05d 为最小宽度，不截断
		s := Slot(cand)
		if stagTable[s] == "" {
			stagTable[s] = cand
			filled++
		}
	}
}

// SlotTag 返回落在指定 slot 的 tag 字符串（形如 "{s:00042}"）。
// 与行同 slot 的辅助 key 一律用 SlotTag(Slot(rowKey)) 作 tag。
func SlotTag(slot uint16) string {
	return stagTable[slot%NumSlots]
}
