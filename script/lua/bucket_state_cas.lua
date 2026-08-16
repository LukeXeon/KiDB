-- @name bucket_state_cas
-- @version 1
-- @keys_desc KEYS[1]=bm_doc(router，集中文档 bm:{table}:{idx})
-- @idempotent false
--
-- 桶状态机 CAS 步进（docs/08 §8.3 第 1/4/5 步）：
-- 校验期望 version → HSET 字段新状态 → version+1。
-- 注意（v7.0 修订）：bm 集中为每索引一文档（bm:{table}:{idx}）——
-- 16384 分片消除（桶不再按行 slot 散布），bm CAS 移出行 Lua 后集中 key 单 slot 可达。
--
-- ARGV: [1] expect_version（-1=不校验）, [2] field, [3] new_value（msgp/十进制串）
-- 返回：{"ok", newVer} / {"stale", curVer}

local cur = tonumber(redis.call('HGET', KEYS[1], 'version') or '0')
local expect = tonumber(ARGV[1])
if expect >= 0 and cur ~= expect then
  return {'stale', tostring(cur)}
end
redis.call('HSET', KEYS[1], ARGV[2], ARGV[3])
local nv = redis.call('HINCRBY', KEYS[1], 'version', 1)
return {'ok', tostring(nv)}
