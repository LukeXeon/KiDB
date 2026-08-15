-- @name replica_refresh
-- @version 1
-- @keys_desc KEYS[1]=replica_key(router，异 slot 副本)
-- @idempotent true
--
-- L4 热桶副本重建（docs/08 §8.4）：读者要么见旧副本要么见新副本——
-- DEL + 批量 ZADD + 版本戳 + 60s 滚动 TTL 在单 slot Lua 内原子完成。
-- 版本戳存于同 slot 伴生 String key（副本本体是 ZSet，单 key 单类型——
-- Redis 不允许 ZSet 挂 Hash 副字段，v4 文档的"@ver Hash 副字段"物理不可达，修订为伴生 key）。
--
-- ARGV: [1] 版本戳（源桶刷新代际）, [2] TTL 毫秒, [3] member 数 N,
--   随后 N×(score, member)
-- 返回：重建的 member 数。

local rkey = KEYS[1]
local ver  = ARGV[1]
local ttl  = tonumber(ARGV[2])
local N    = tonumber(ARGV[3])

redis.call('DEL', rkey)
local p = 4
for i = 1, N do
  redis.call('ZADD', rkey, tonumber(ARGV[p]), ARGV[p+1])
  p = p + 2
end
redis.call('SET', rkey .. '@ver', ver, 'PX', ttl)
redis.call('PEXPIRE', rkey, ttl)
return N
