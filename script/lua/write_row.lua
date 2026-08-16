-- @name write_row
-- @version 6
-- @keys_desc KEYS[1]=row_key(router); KEYS[2..n-2]=bucket/bm_keys; KEYS[n-1]=exp; KEYS[n]=rcpt
-- @idempotent true
--
-- 单行写入（INSERT/UPDATE/DELETE/UPSERT 全分支，docs/05 §5.1）。
-- 原子完成：读旧行 → 版本/状态 CAS 预检 → 撤旧索引 → 写新行 → 撤幽灵字段 →
-- 建新索引 → 登记过期 → 维护回执。（v4：cnt 移除；v5：ttlms=-2 保留分支；
-- v6：撤字段面 D（UPDATE 置 NULL 必须 HDEL 旧字段，否则旧值幽灵残留）+
-- 日志容量/回执宽限 ARGV 化（不再硬编码，随 tuning.toml 生效））
--
-- ARGV 协议（v3 起描述符 8 字段；桶段 KEYS[2..n-3] 相对序号 1 起；0 = 无）：
--   [1] op: "W"=写入（含 upsert 分支） / "D"=删除
--   [2] pk
--   [3] ttl_ms（"0"=无 TTL；"-2"=保留现状（UPDATE 不提 _ttl 时——行 TTL 与
--        exp 登记照旧，回执按新成员重写、宽限=行剩余 PTTL+回执宽限））
--   [4] now_sec（过期登记册 score 基准，秒）
--   [5] expected_old_ver（"-1"=不校验；与行内 _ver 不符返回 stale）
--   [6] M = 索引描述符个数
--   [7] logCap = 异步日志容量上限（tuning txguard.async_log_capacity）
--   [8] graceMs = 回执宽限毫秒（tuning sweeper.receipt_grace_ms）
--   每个索引 8 字段（顺序消费，自 ARGV[9] 起）：
--     kind("E"=等值/"R"=范围/"L"=字典序副本/"A"=异步日志)
--     undo_key_idx, undo_member,
--     redo_key_idx, redo_member, redo_score（范围桶用；其余为 "0"）
--     bm_key_idx, bm_version —— BucketMap 分片版本 CAS（分裂状态一致性，
--       docs/08 §8.3：预检不符返回 stale；0 = 该校验位关闭）
--     异步（A）：无 undo；redo_key 为日志 key，redo_member = pk\x1f旧值\x1f新值
--   W 追加：F = 字段数，F×(field, value)
--   W 追加：D = 撤字段数，D×field（旧有新无 → HDEL；UPDATE 置 NULL 语义面）
--   W 追加：U = 唯一预约数，U×(index_id, reservation_key)（记入回执 __uniq: 字段）
--
-- 返回：{"ok", old_ver, new_ver} / {"stale", old_ver} / {"log_full", old_ver}
--
-- 跨脚本不变式（docs/05 §5.6、docs/07 §7.3、docs/08 §8.3）：
--   - 复活语义：旧行空但 rcpt 存在 = 主键复活——exp 已被本次覆盖，
--     sweeper 侧由 sweep_batch.lua 的 "ZSCORE exp > now 则跳过" 复查保证
--     不清理活行索引。
--   - ZSet member 去重天然幂等：重复应用同一写入不产生重复索引条目。
--   - bm 版本 CAS 必须在一切写操作之前（Lua 无回滚，中途返回即部分提交）；
--     分裂窗口的双写规则由调用方按 SPLITTING/DRAINING 展开 redo 描述符。

local rowkey  = KEYS[1]
local nkeys   = #KEYS
local expkey  = KEYS[nkeys-1]
local rcptkey = KEYS[nkeys]

local op        = ARGV[1]
local pk        = ARGV[2]
local ttlms     = tonumber(ARGV[3])
local now       = tonumber(ARGV[4])
local expectOld = tonumber(ARGV[5])
local M         = tonumber(ARGV[6])
local logCap    = tonumber(ARGV[7])
local graceMs   = tonumber(ARGV[8])

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

-- 解析索引描述符（8 字段）
local p = 9
local descs = {}
for i = 1, M do
  descs[i] = {
    kind       = ARGV[p],
    undoKey    = tonumber(ARGV[p+1]),
    undoMember = ARGV[p+2],
    redoKey    = tonumber(ARGV[p+3]),
    redoMember = ARGV[p+4],
    redoScore  = tonumber(ARGV[p+5]),
    bmKey      = tonumber(ARGV[p+6]),
    bmVer      = tonumber(ARGV[p+7]),
  }
  p = p + 8
end

-- 预检一：BucketMap 版本 CAS（分裂状态一致性；先于此后的任何写操作）
for i = 1, M do
  local d = descs[i]
  if d.bmKey > 0 then
    local v = tonumber(redis.call('HGET', KEYS[1 + d.bmKey], 'version') or '0')
    if v ~= d.bmVer then
      return {'stale', tostring(oldVer)}
    end
  end
end

-- 预检二：异步日志容量（Lua 无回滚，必须先查后写；容量由调用方按 tuning 传入）
for i = 1, M do
  local d = descs[i]
  if d.kind == 'A' and d.redoKey > 0 then
    if redis.call('LLEN', KEYS[1 + d.redoKey]) >= logCap then
      return {'log_full', tostring(oldVer)}
    end
  end
end

if op == 'D' then
  -- 命中已过期行（old 空）→ 0 rows affected：索引残留给 sweeper，不动回执
  if oldExists then
    for i = 1, M do
      local d = descs[i]
      if d.undoKey > 0 then
        redis.call('ZREM', KEYS[1 + d.undoKey], d.undoMember)
      end
    end
    -- 异步索引：删除也走日志（墓碑条目，新值为空；容量已预检）
    for i = 1, M do
      local d = descs[i]
      if d.kind == 'A' and d.redoKey > 0 then
        redis.call('RPUSH', KEYS[1 + d.redoKey], d.redoMember .. string.char(31) .. tostring(oldVer))
      end
    end
    redis.call('DEL', rowkey)
    redis.call('ZREM', expkey, pk)
    redis.call('DEL', rcptkey)
  end
  return {'ok', tostring(oldVer), ''}
end

-- ============ op == 'W' ============

-- 撤旧索引（含主键复活：撤销条目由调用方按旧行/旧回执展开进描述符；异步无撤销——日志追加即覆盖语义）
for i = 1, M do
  local d = descs[i]
  if d.undoKey > 0 then
    redis.call('ZREM', KEYS[1 + d.undoKey], d.undoMember)
  end
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

-- 建新索引（同步分支；SPLITTING 双写/DRAINING 仅子桶由描述符展开给出）
for i = 1, M do
  local d = descs[i]
  if d.redoKey > 0 and d.kind ~= 'A' then
    redis.call('ZADD', KEYS[1 + d.redoKey], d.redoScore, d.redoMember)
  end
end

local newVer = redis.call('HINCRBY', rowkey, '_ver', 1)

-- 异步索引：追加变更日志（条目 = pk\x1f旧值\x1f新值\x1fver；Indexer 消费 docs/05 §5.2）
for i = 1, M do
  local d = descs[i]
  if d.kind == 'A' and d.redoKey > 0 then
    redis.call('RPUSH', KEYS[1 + d.redoKey], d.redoMember .. string.char(31) .. tostring(newVer))
  end
end

-- 过期登记册 + 行 TTL + 回执
if ttlms == -2 then
  -- UPDATE 保留语义：行 TTL 不动（Redis HSET 本就不清 TTL），exp 已登记照旧；
  -- 新行（旧不存在/已过期）无 TTL 可保 → 登记 +inf；回执按新成员重写
  redis.call('DEL', rcptkey)
  if not oldExists then
    redis.call('ZADD', expkey, '+inf', pk)
  end
  for i = 1, M do
    local d = descs[i]
    if d.redoKey > 0 and d.kind ~= 'A' then
      redis.call('HSET', rcptkey, 'idx:' .. i,
        KEYS[1 + d.redoKey] .. string.char(31) .. d.redoMember)
    end
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
  redis.call('ZADD', expkey, now + ttlms / 1000, pk)
  redis.call('PEXPIRE', rowkey, ttlms)
  redis.call('DEL', rcptkey)
  for i = 1, M do
    local d = descs[i]
    if d.redoKey > 0 and d.kind ~= 'A' then
      redis.call('HSET', rcptkey, 'idx:' .. i,
        KEYS[1 + d.redoKey] .. string.char(31) .. d.redoMember)
    end
  end
  local U = tonumber(ARGV[p]); p = p + 1
  for i = 1, U do
    redis.call('HSET', rcptkey, '__uniq:' .. ARGV[p], ARGV[p+1])
    p = p + 2
  end
  redis.call('PEXPIRE', rcptkey, ttlms + graceMs)
else
  redis.call('ZADD', expkey, '+inf', pk)
  redis.call('PERSIST', rowkey)
  redis.call('DEL', rcptkey)
end

return {'ok', tostring(oldVer), tostring(newVer)}
