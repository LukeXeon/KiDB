-- @name catalog_set
-- @version 1
-- @keys_desc KEYS[1]=catalog_key(router)
-- @idempotent false
--
-- Catalog 保存的原子 CAS（docs/06 §6.1）：校验期望 _ver → HSET def → _ver+1。
-- 替代读-改-写（并发 DDL 会丢更新——两个 CREATE INDEX 同时读旧 def 各写一半）。
--
-- ARGV: [1] def（msgp 编码）, [2] expect_ver（-1=不校验）, [3] fmtv
-- 返回：{"ok", new_ver} / {"stale", cur_ver}

local cur = tonumber(redis.call('HGET', KEYS[1], '_ver') or '0')
local expect = tonumber(ARGV[2])
if expect >= 0 and cur ~= expect then
  return {'stale', tostring(cur)}
end
redis.call('HSET', KEYS[1], 'def', ARGV[1], '_fmtv', ARGV[3])
local nv = redis.call('HINCRBY', KEYS[1], '_ver', 1)
return {'ok', tostring(nv)}
