-- @name lock_renew
-- @version 1
-- @keys_desc KEYS[1]=lock_key(router)
-- @idempotent true
-- 锁续期（token 比对，docs/08 §8.5 watchdog 用）：只续自己的锁。
-- ARGV[1]=token, ARGV[2]=TTL 毫秒。
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
