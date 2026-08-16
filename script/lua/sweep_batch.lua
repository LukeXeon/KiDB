-- @name sweep_batch
-- @version 3
-- @keys_desc KEYS[1..N]=row_key（KEYS[1] 为路由 key）; KEYS[N+1..2N]=rcpt_key
-- @idempotent true
--
-- Sweeper 行 slot 清扫批（docs/07 §7.3，v7.0 收窄形态）。
-- v7.0：索引桶/登记册移出（异 slot，按值/索引寻址）——本脚本只承担行 slot 原子面：
-- 活性复查 + 回执删除。桶/登记册清理由调用方在本脚本确认死亡后于客户端段完成
-- （版本戳精确 member，交错安全）。
--
-- 活性复查（v7 形态）：行 PTTL > 0 或 -1 = 行存活（复活/时钟偏斜）→ 跳过且
-- **不动回执**（活行的回执由写入方维护）；PTTL == -2（行物理不存在）→ 删回执。
-- 回执 DEL 与复查必须同脚本原子——非原子会在"复查后复活"窗口误删活行新回执。
--
-- ARGV 协议：
--   [1] N = 条目数
--   每条目：[pk, rowkey_idx, rcptkey_idx]（idx 为相对各自段的起 1 序号）
-- 返回：实际清扫（确认死亡并删回执）的 pk 列表。

local N = tonumber(ARGV[1])

local p = 2
local cleaned = {}
for e = 1, N do
  local pk       = ARGV[p]
  local rowIdx   = tonumber(ARGV[p+1])
  local rcptIdx  = tonumber(ARGV[p+2])
  p = p + 3
  local pttl = redis.call('PTTL', KEYS[rowIdx])
  if pttl == -2 then -- 行物理不存在 → 回执可清
    redis.call('DEL', KEYS[N + rcptIdx])
    cleaned[#cleaned + 1] = pk
  end
end
return cleaned
