-- @name lock_release
-- @version 1
-- @keys_desc KEYS[1]=lock_key(router)
-- @idempotent true
-- 锁释放（token 比对，docs/08 §8.5）：只删自己的锁。
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
