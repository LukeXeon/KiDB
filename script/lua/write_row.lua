-- @name write_row
-- @version 7
-- @keys_desc KEYS[1]=row_key(router); KEYS[2]=rcpt_key; KEYS[3..2+A]=async_log_keys
-- @idempotent true
--
-- 行本地写（INSERT/UPDATE/DELETE/UPSERT 全分支，docs/05 §5.1 v7.0 两段写协议）。
-- v7.0 收窄：本脚本只承担"行 + 回执 + 异步日志"的单 slot 原子面——
-- 索引桶/过期登记册移出（按值/索引寻址，异 slot），由调用方在同一 pipeline
-- 随后的命令段完成（成员带版本戳 pk\x1fver，并发交错互不误撤，不漏方向得证；
-- stale 时同 pipeline 索引段的产物必为"多"——读取去重/回表过滤/对账清理兜底）。
--
-- ARGV 协议（v7）：
--   [1] op: "W"=写入（含 upsert 分支） / "D"=删除
--   [2] pk
--   [3] ttl_ms（"0"=无 TTL；"-2"=保留现状（UPDATE 不提 _ttl：行 TTL 照旧，
--        回执按新成员重写、宽限=行剩余 PTTL+回执宽限））
--   [4] expected_old_ver（"-1"=不校验；与行内 _ver 不符返回 stale）
--   [5] logCap = 异步日志容量上限（tuning txguard.async_log_capacity）
--   [6] graceMs = 回执宽限毫秒（tuning sweeper.receipt_grace_ms）
--   [7] A = 异步日志描述符个数；每个 2 字段（自 ARGV[8] 起）：
--         log_key_idx（相对 KEYS[3] 起，1 起）、redo_member（pk\x1f旧值\x1f新值）
--   W 追加：R = 回执重建条目数，R×(bucket_key, member)——回执记撤销信息
--         （member 由本脚本追加 \x1fnewVer 后写入；调用方索引段同规则建 member）
--   W 追加：F = 字段数，F×(field, value)
--   W 追加：D = 撤字段数，D×field（旧有新无 → HDEL；UPDATE 置 NULL 语义面）
--   W 追加：U = 唯一预约数，U×(index_id, reservation_key)（记入回执 __uniq: 字段）
--
-- 返回：{"ok", old_ver, new_ver} / {"stale", old_ver} / {"log_full", old_ver}
-- D 变体返回：{"ok", existed}（existed "1"=已删除行/"0"=命中已过期行 0 rows affected）
--
-- 跨脚本不变式（docs/05 §5.6、docs/07 §7.3、docs/08 §8.3）：
--   - 复活语义：旧行空但 rcpt 存在 = 主键复活——撤销由调用方按旧回执展开进
--     索引段（版本戳精确 member，幂等）；
--   - stale 预检必须在一切写操作之前（Lua 无回滚，中途返回即部分提交）。

local rowkey  = KEYS[1]
local rcptkey = KEYS[2]

local op        = ARGV[1]
local pk        = ARGV[2]
local ttlms     = tonumber(ARGV[3])
local expectOld = tonumber(ARGV[4])
local logCap    = tonumber(ARGV[5])
local graceMs   = tonumber(ARGV[6])
local A         = tonumber(ARGV[7])

-- 读旧行与旧 _ver（docs/05 §5.6 幂等校验）
local old = redis.call('HGETALL', rowkey)
local oldExists = #old > 0
local oldVer = 0
if oldExists then
  for i = 1, #old, 2 do
    if old[i] == '_ver' then oldVer = tonumber(old[i+1]) break end
  end
end
if expectOld >= 0 and oldVer ~= expectOld then
  return {'stale', tostring(oldVer)}
end

-- 解析异步日志描述符（2 字段）+ 容量预检（Lua 无回滚，必须先查后写）
local p = 8
local adescs = {}
for i = 1, A do
  adescs[i] = { logKey = tonumber(ARGV[p]), redoMember = ARGV[p+1] }
  p = p + 2
end
for i = 1, A do
  local d = adescs[i]
  if redis.call('LLEN', KEYS[2 + d.logKey]) >= logCap then
    return {'log_full', tostring(oldVer)}
  end
end

if op == 'D' then
  -- 命中已过期行（old 空）→ 0 rows affected：索引残留给 sweeper/对账，不动回执
  if oldExists then
    -- 异步索引：删除也走日志（墓碑条目，新值为空；容量已预检）
    for i = 1, A do
      local d = adescs[i]
      redis.call('RPUSH', KEYS[2 + d.logKey], d.redoMember .. string.char(31) .. tostring(oldVer))
    end
    redis.call('DEL', rowkey)
    redis.call('DEL', rcptkey)
  end
  if oldExists then
    return {'ok', '1'}
  end
  return {'ok', '0'}
end

-- ============ op == 'W' ============

-- 解析回执重建条目（R×(bucket_key, member)）
local R = tonumber(ARGV[p]); p = p + 1
local rdescs = {}
for i = 1, R do
  rdescs[i] = { bucket = ARGV[p], member = ARGV[p+1] }
  p = p + 2
end

-- 写新行字段
local F = tonumber(ARGV[p]); p = p + 1
for i = 1, F do
  redis.call('HSET', rowkey, ARGV[p], ARGV[p+1])
  p = p + 2
end

-- 撤幽灵字段（旧有新无 = UPDATE 置 NULL；HSET 不覆盖即残留，必须显式 HDEL）
local D = tonumber(ARGV[p]); p = p + 1
if D > 0 then
  local dargs = {}
  for i = 1, D do
    dargs[i] = ARGV[p]
    p = p + 1
  end
  redis.call('HDEL', rowkey, unpack(dargs))
end

local newVer = redis.call('HINCRBY', rowkey, '_ver', 1)

-- 异步索引：追加变更日志（条目 = pk\x1f旧值\x1f新值\x1fver；Indexer 消费 docs/05 §5.2）
for i = 1, A do
  local d = adescs[i]
  redis.call('RPUSH', KEYS[2 + d.logKey], d.redoMember .. string.char(31) .. tostring(newVer))
end

-- 回执重建（撤销信息 = 桶key + 精确 member（追加 newVer 版本戳））+ 行 TTL
if ttlms == -2 then
  -- UPDATE 保留语义：行 TTL 不动（Redis HSET 本就不清 TTL）；
  -- 回执按新成员重写；宽限 = 行剩余 PTTL + grace
  redis.call('DEL', rcptkey)
  for i = 1, R do
    local d = rdescs[i]
    redis.call('HSET', rcptkey, 'idx:' .. i,
      d.bucket .. string.char(31) .. d.member .. string.char(31) .. tostring(newVer))
  end
  local U = tonumber(ARGV[p]); p = p + 1
  for i = 1, U do
    redis.call('HSET', rcptkey, '__uniq:' .. ARGV[p], ARGV[p+1])
    p = p + 2
  end
  local remain = redis.call('PTTL', rowkey)
  if remain > 0 then
    redis.call('PEXPIRE', rcptkey, remain + graceMs)
  end
elseif ttlms > 0 then
  redis.call('PEXPIRE', rowkey, ttlms)
  redis.call('DEL', rcptkey)
  for i = 1, R do
    local d = rdescs[i]
    redis.call('HSET', rcptkey, 'idx:' .. i,
      d.bucket .. string.char(31) .. d.member .. string.char(31) .. tostring(newVer))
  end
  local U = tonumber(ARGV[p]); p = p + 1
  for i = 1, U do
    redis.call('HSET', rcptkey, '__uniq:' .. ARGV[p], ARGV[p+1])
    p = p + 2
  end
  redis.call('PEXPIRE', rcptkey, ttlms + graceMs)
else
  redis.call('PERSIST', rowkey)
  redis.call('DEL', rcptkey)
end

return {'ok', tostring(oldVer), tostring(newVer)}
