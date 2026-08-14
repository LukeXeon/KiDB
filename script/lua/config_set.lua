-- @name config_set
-- @version 1
-- @keys_desc KEYS[1]=cfg:global(router)
-- @idempotent true
--
-- SET GLOBAL 的 _ver CAS 原子更新（docs/10 §10.2）：
-- 校验期望 _ver → HSET 新值 → _ver+1 → 追加 _audit。
-- ARGV: [1] 变量名, [2] 编码值, [3] 期望 _ver（-1 = 不校验）, [4] 修改者, [5] 时间戳
-- 返回：{"ok", new_ver} 或 {"stale", cur_ver}

local key = KEYS[1]
local name  = ARGV[1]
local value = ARGV[2]
local expectVer = tonumber(ARGV[3])

local cur = tonumber(redis.call('HGET', key, '_ver') or '0')
if expectVer >= 0 and cur ~= expectVer then
  return {'stale', tostring(cur)}
end
redis.call('HSET', key, name, value)
local newVer = redis.call('HINCRBY', key, '_ver', 1)
redis.call('HSET', key, '_audit', ARGV[4] .. '|' .. ARGV[5])
return {'ok', tostring(newVer)}
