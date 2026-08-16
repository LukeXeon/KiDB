-- @name sweep_batch
-- @version 2
-- @keys_desc KEYS[1]=exp(router); KEYS[2..]=per-entry: rcpt_key, bucket_keys...（v2：cnt 计数器移除）
-- @idempotent true
--
-- Sweeper 单 slot 清扫批（docs/07 §7.3）。
-- 关键不变式（docs/05 写入路径跨脚本约定）：清扫前必须复查 exp 中 pk 的 score——
-- score > now 说明行已复活（写路径覆盖了登记项），跳过，否则会把活行的索引误清、
-- 把新回执误删。
--
-- ARGV 协议：
--   [1] now_sec
--   [2] N = 条目数
--   每条目：[pk, rcpt_key_idx, bucket_count, (bucket_key_idx, member) × bucket_count]
--   key_idx 是相对 KEYS[3..] 段的起 1 序号。
-- 返回：实际清扫条数。

local expkey = KEYS[1]
local now = tonumber(ARGV[1])
local N   = tonumber(ARGV[2])

local p = 3
local swept = 0
for e = 1, N do
  local pk        = ARGV[p]
  local rcptIdx   = tonumber(ARGV[p+1])
  local nbuck     = tonumber(ARGV[p+2])
  p = p + 3
  -- 复查：登记项 score（复活拦截 + 重复清扫幂等）
  local score = redis.call('ZSCORE', expkey, pk)
  local alive = false
  if score then
    if tonumber(score) > now then alive = true end -- 复活：score 在未来
  else
    alive = true -- 已被清扫/从未登记 → 幂等跳过
  end
  if alive then
    p = p + nbuck * 2
  else
    for i = 1, nbuck do
      local bIdx   = tonumber(ARGV[p])
      local member = ARGV[p+1]
      p = p + 2
      redis.call('ZREM', KEYS[1 + bIdx], member)
    end
    redis.call('DEL', KEYS[1 + rcptIdx])
    redis.call('ZREM', expkey, pk)
    swept = swept + 1
  end
end
return swept
