-- @name write_row
-- @version 1
-- @keys_desc KEYS[1]=row_key(router); KEYS[2..n-3]=bucket_keys; KEYS[n-2]=exp; KEYS[n-1]=cnt; KEYS[n]=rcpt
-- @idempotent true
--
-- 单行写入（INSERT/UPDATE/DELETE/UPSERT 全分支，docs/05 §5.1）。
-- 原子完成：读旧行 → 撤旧索引 → 写新行 → 建新索引 → 登记过期 → 维护回执与计数。
--
-- ARGV 协议（描述符中的桶序号是桶段 KEYS[2..n-3] 的相对序号，1 起；0 = 无）：
--   [1] op: "W"=写入（含 upsert 分支） / "D"=删除
--   [2] pk
--   [3] ttl_ms（"0"=无 TTL）
--   [4] now_sec（过期登记册 score 基准，秒）
--   [5] expected_old_ver（"-1"=不校验；与行内 _ver 不符返回 stale）
--   [6] M = 索引描述符个数
--   每个索引 6 字段（顺序消费）：
--     kind("E"=等值/"R"=范围/"L"=字典序副本)  —— v1 三分支撤销/重建同为 ZREM/ZADD，
--                                              kind 仅作协议自描述与调试可读性
--     undo_key_idx, undo_member,
--     redo_key_idx, redo_member, redo_score（范围桶用；其余为 "0"）
--   W 追加：F = 字段数，F×(field, value)
--   W 追加：U = 唯一预约数，U×(index_id, reservation_key)（记入回执 __uniq: 字段）
--
-- 返回：{"ok", old_ver, new_ver} 或 {"stale", old_ver}
--
-- 跨脚本不变式（docs/05 §5.6、docs/07 §7.3）：
--   - 复活语义：旧行空但 rcpt 存在 = 主键复活，cnt 不 INCR（原 INCR 仍成立，
--     exp 已被本次覆盖，sweeper 不会再 DECR）——平衡由 sweep_batch.lua 的
--     "ZSCORE exp > now 则跳过" 复查保证，无复查则会 DECR 错账 + 误删新回执。
--   - ZSet member 去重天然幂等：重复应用同一写入不产生重复索引条目。

local rowkey  = KEYS[1]
local nkeys   = #KEYS
local expkey  = KEYS[nkeys-2]
local cntkey  = KEYS[nkeys-1]
local rcptkey = KEYS[nkeys]

local op        = ARGV[1]
local pk        = ARGV[2]
local ttlms     = tonumber(ARGV[3])
local now       = tonumber(ARGV[4])
local expectOld = tonumber(ARGV[5])
local M         = tonumber(ARGV[6])

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

-- 解析索引描述符
local p = 7
local descs = {}
for i = 1, M do
  descs[i] = {
    undoKey    = tonumber(ARGV[p+1]),
    undoMember = ARGV[p+2],
    redoKey    = tonumber(ARGV[p+3]),
    redoMember = ARGV[p+4],
    redoScore  = tonumber(ARGV[p+5]),
  }
  p = p + 6
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
    redis.call('DEL', rowkey)
    redis.call('ZREM', expkey, pk)
    redis.call('DECR', cntkey)
    redis.call('DEL', rcptkey)
  end
  return {'ok', tostring(oldVer), ''}
end

-- ============ op == 'W' ============

-- 撤旧索引（含主键复活：撤销条目由调用方按旧行/旧回执展开进描述符）
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

-- 建新索引
for i = 1, M do
  local d = descs[i]
  if d.redoKey > 0 then
    redis.call('ZADD', KEYS[1 + d.redoKey], d.redoScore, d.redoMember)
  end
end

local newVer = redis.call('HINCRBY', rowkey, '_ver', 1)

-- 计数：仅"全新插入"（旧行空且无回执）INCR；复活不 INCR（见头部不变式）
local rcptExists = redis.call('EXISTS', rcptkey) == 1
if (not oldExists) and (not rcptExists) then
  redis.call('INCR', cntkey)
end

-- 过期登记册 + 行 TTL + 回执
if ttlms > 0 then
  redis.call('ZADD', expkey, now + ttlms / 1000, pk)
  redis.call('PEXPIRE', rowkey, ttlms)
  redis.call('DEL', rcptkey)
  for i = 1, M do
    local d = descs[i]
    if d.redoKey > 0 then
      redis.call('HSET', rcptkey, 'idx:' .. i,
        KEYS[1 + d.redoKey] .. string.char(31) .. d.redoMember)
    end
  end
  local U = tonumber(ARGV[p]); p = p + 1
  for i = 1, U do
    redis.call('HSET', rcptkey, '__uniq:' .. ARGV[p], ARGV[p+1])
    p = p + 2
  end
  redis.call('PEXPIRE', rcptkey, ttlms + 300000) -- receipt_grace_period
else
  redis.call('ZADD', expkey, '+inf', pk)
  redis.call('PERSIST', rowkey)
  redis.call('DEL', rcptkey)
end

return {'ok', tostring(oldVer), tostring(newVer)}
