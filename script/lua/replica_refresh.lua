-- @name replica_refresh
-- @version 2
-- @keys_desc KEYS[1]=replica_key(router，异 slot 副本)
-- @idempotent true
--
-- L4 热桶副本重建（docs/08 §8.4）：读者要么见旧副本要么见新副本——
-- DEL + 批量 ZADD + 60s 滚动 TTL 在单 slot Lua 内原子完成。
-- 副本死活由读侧同 pipeline EXISTS 判定回退源桶承载
-- （v2 起 @ver 伴生 key 移除——只写不读的 vestigial，cnt 同款病）。
--
-- ARGV: [1] TTL 毫秒, [2] member 数 N, 随后 N×(score, member)
-- 返回：重建的 member 数。

local rkey = KEYS[1]
local ttl  = tonumber(ARGV[1])
local N    = tonumber(ARGV[2])

redis.call('DEL', rkey)
local p = 3
for i = 1, N do
  redis.call('ZADD', rkey, tonumber(ARGV[p]), ARGV[p+1])
  p = p + 2
end
redis.call('PEXPIRE', rkey, ttl)
return N
