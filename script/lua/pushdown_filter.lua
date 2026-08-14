-- @name pushdown_filter
-- @version 1
-- @keys_desc KEYS[1..n]=row_keys(同 slot；KEYS[1] 为路由 key)
-- @idempotent true
--
-- 服务端谓词下推（docs/04 §4.2）：在节点内完成"回表 → 谓词过滤"，
-- 网络只传命中行。白名单形态：单列等值集合 或 单列 score 区间。
--
-- ARGV 协议：
--   [1] 谓词列名
--   [2] 模式："always"（仅删空行）/ "eq" / "range"
--   eq:    [3] = 命中值个数 K，随后 K 个编码值
--   range: [3]=lo, [4]=hi, [5]=loOpen(0/1), [6]=hiOpen(0/1)
-- 返回：扁平数组 [pk, fieldCount, (field, value)×n, ...]——pk 从行 key 的 hash tag
-- 提取（d:{table}:{pk} 格式由 keycodec 保证）；客户端校验路径互为参照（P4 对拍）。

local field = ARGV[1]
local mode  = ARGV[2]

local out = {}
for i = 1, #KEYS do
  local row = redis.call('HGETALL', KEYS[i])
  if #row > 0 then
    -- 取谓词列值
    local val = nil
    for j = 1, #row, 2 do
      if row[j] == field then val = row[j+1] break end
    end
    local hit = false
    if mode == 'always' then
      hit = true
    elseif val ~= nil and val ~= false then
      if mode == 'eq' then
        local K = tonumber(ARGV[3])
        for k = 1, K do
          if val == ARGV[3 + k] then hit = true break end
        end
      elseif mode == 'range' then
        local lo = tonumber(ARGV[3])
        local hi = tonumber(ARGV[4])
        local loOpen = ARGV[5] == '1'
        local hiOpen = ARGV[6] == '1'
        local f = tonumber(val)
        if f ~= nil then
          hit = true
          if loOpen then if f <= lo then hit = false end elseif f < lo then hit = false end
          if hiOpen then if f >= hi then hit = false end elseif f > hi then hit = false end
        end
      end
    end
    if hit then
      local pk = string.match(KEYS[i], '{(.-)}') or KEYS[i]
      out[#out + 1] = pk
      out[#out + 1] = #row / 2
      for j = 1, #row do
        out[#out + 1] = row[j]
      end
    end
  end
end
return out
