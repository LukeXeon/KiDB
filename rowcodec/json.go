package rowcodec

import (
	"encoding/json"
	"fmt"
)

// json.go：JSON 列的存储形态（docs/03 §3.4）——**归一化文本直存**。
//
// 形态裁决（v6.x review 用户裁决，替代 v6.0~f7690a6 的 msgpack 二进制）：
// msgpack 化对文本主导内容只有 ~15% 体积收益，代价是手卷双格式编解码器
// （正确性风险面）+ HGET 二进制不可读（运维可观测性归零）+ >2^53 精度声明
// 负担；真要大压缩率应上 zstd 而非 msgpack。故回退为文本直存，MySQL 语义
// 对齐由"写入时解析归一化"承接（Unmarshal→Marshal：key 排序、数字 float64
// 归一、重复 key 后写胜——与 MySQL 二进制 JSON 同族行为）：
//   - 写入：非法 JSON 报错；归一化重序列化后存储（读回格式即规范格式）；
//   - 读出：原文返回（存储形态已是规范文本，零转换成本）；
//   - 运维面：HGET 直读文本（Redis 生态直觉恢复）。
//
// 已知格式差异（如实声明）：数字按 Go float64 最短形式输出（1.0 → "1"），
// 与 MySQL 二进制 JSON 的 DOUBLE 打印（"1.0"）有展示级差异；>2^53 整数
// 精度不担保（float64 归一纪律，与 score 同族）。

// EncodeJSON 把 gms JSON 列值归一化为规范 JSON 文本。
// 输入形态：types.JSONDocument（gms Convert 产物）/ string / []byte / 已解析值。
func EncodeJSON(v any) (string, error) {
	var doc any
	switch t := v.(type) {
	case nil:
		return "", nil
	case interface{ ToInterface() (interface{}, error) }: // sql.JSONWrapper（types.JSONDocument）
		d, err := t.ToInterface()
		if err != nil {
			return "", err
		}
		doc = d
	case string:
		if err := json.Unmarshal([]byte(t), &doc); err != nil {
			return "", fmt.Errorf("rowcodec: JSON 文本非法: %w", err)
		}
	case []byte:
		if err := json.Unmarshal(t, &doc); err != nil {
			return "", fmt.Errorf("rowcodec: JSON 文本非法: %w", err)
		}
	default:
		doc = t
	}
	// 归一化重序列化：key 排序 + 数字 float64 归一 + 重复 key 后写胜
	// （Unmarshal 已完成后两者；Marshal 对 map key 排序）。
	b, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// DecodeJSON 读出即规范文本（写入侧已归一化——存储形态 = 返回形态）。
func DecodeJSON(s string) (any, error) {
	return s, nil
}
