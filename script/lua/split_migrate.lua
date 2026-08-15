-- @name split_migrate
-- @version 1
-- @keys_desc KEYS[1]=parent_bucket(router，带 stag); KEYS[2..C+1]=children; KEYS[C+2..]=rowkeys(同 slot)
-- @idempotent true
--
-- 分裂搬迁批（docs/08 §8.3 第 3 步）：逐 member EXISTS 行key
-- （已过期直接丢弃，顺路清理）→ ZADD 目标子桶 → ZREM 父桶。
-- 目标子桶由调用方（Controller）按分裂规则计算后传入——Lua 不做哈希决策。
--
-- ARGV: [1] 子桶数 C；[2] member 数 N；每 member 4 字段：
--   member, target_child_idx(1..C，对应 KEYS[1+idx]), score, rowkey_idx(相对 KEYS[C+2..] 段，1 起)
-- 返回：实际搬迁 member 数。

local parent = KEYS[1]
local C = tonumber(ARGV[1])
local N = tonumber(ARGV[2])
local p = 3
local moved = 0
for i = 1, N do
  local member = ARGV[p]
  local target = tonumber(ARGV[p+1])
  local score  = tonumber(ARGV[p+2])
  local rkIdx  = tonumber(ARGV[p+3])
  p = p + 4
  local child = KEYS[1 + target]
  local rowkey = KEYS[1 + C + rkIdx]
  if redis.call('EXISTS', rowkey) == 1 then
    redis.call('ZADD', child, score, member)
  end
  redis.call('ZREM', parent, member)
  moved = moved + 1
end
return moved
