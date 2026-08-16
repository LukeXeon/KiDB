# 07 · TTL 与过期清扫（含全表遍历的 SCAN 替代机制）

> **TiDB 参照**：TiDB 行 TTL = `pkg/ttl` 后台扫描删除作业（SplitScanRanges 按 region 切片，task 落表断点续扫）。
> KiDB 的有利差异：**Redis 原生 key TTL 直接回收行本身**（TiKV 没有的能力红利），Sweeper 只需清扫索引残留；
> 不利差异：不能 SCAN（TiDB TTL 作业扫描依赖有序迭代）→ 清扫由 exp 登记册驱动，而不是 keyspace 遍历。

## 7.1 SQL 表达

`_ttl` 保留列（秒，BIGINT）：

- `>0`：行级 TTL；
- `0`：显式无 TTL（覆盖表级 `default_ttl`）；
- `NULL`（不提及）：承表级 `default_ttl`；
- `<0`：软删除（立即过期走清扫）；
- UPDATE 不提 `_ttl`：**保留行当前 TTL**（write_row.lua `ttlms=-2` 分支——
  行 TTL 与登记册照旧，回执按新成员重写；新行无可保则登记不过期）。

表级 `default_ttl` 在 Catalog 声明。读出 = **剩余 TTL 秒**（PTTL 自省，-1 = 无 TTL）：
`SELECT _ttl FROM t WHERE ...` 显式投影即得；`SELECT *` 也含 `_ttl` 列
（gms 无隐藏列机制，`*` 展开为显式投影——这是与"SELECT * 不含伪列"初衷的
诚实偏差；代价：含 `_ttl` 的投影逐行 PTTL 搭同一 pipeline、行级近缓存对该
投影形态自动绕过——hotkey_row_cache 默认关闭，取舍可控）。

**保留列纪律**：`_` 前缀是引擎元数据列的命名空间（生态惯例：MongoDB `_id`、ES `_source`、TiDB `_tidb_rowid`）——**DDL 拒绝用户定义 `_` 开头的列**（[02](02-SQL服务器.md) §2.4 校验），伪列冲突从规则上消失。SQL 面只暴露 `_ttl`；`_ver` 为纯内部列（幂等校验/维表刷新用），不可 SELECT，不出现在任何文档化的 SQL 面。

## 7.2 exp 过期登记册（本方案的关键复用结构）

`exp:{table}:{stag}`（ZSet，member=pk，score=到期时间戳，无 TTL=+inf）由写入 Lua 维护（[05](05-写入路径.md) §5.1 第 5 步），**每一行都有登记**。它同时支撑四个功能：

| 功能 | 命令 | 说明 |
|---|---|---|
| COUNT(*) 精确语义 | `ZCOUNT exp (now, +inf)` 逐 slot 汇总 | 已过期未清扫的行 score≤now 不计入 |
| Sweeper 到期发现 | `ZRANGEBYSCORE exp -inf (now LIMIT 0 512` | 有界批取 |
| **全表遍历** | `ZRANGE exp i i+511` 分页 | 替代 `SCAN MATCH`（§7.4） |
| 维表加载 / 对账抽样 / DDL 回填 | 同上 | [04](04-查询路径.md) §4.4、[06](06-元数据与Schema演进.md) §6.3、[12](12-测试方案.md) §12.8 |

登记册本身无 TTL，成员由 Sweeper 清扫移除；16384 个登记册按 slot 天然散列，单 key 成员数 ≈ 总行数/16384。

**容量账与细分机制（诚实声明）**：登记册是唯一不参与桶分裂的 ZSet，随表行数线性增长。按每成员约 100B（pk + skiplist/dict 开销）估算：1 亿行 ≈ 6k 成员/册（~0.6MB，舒适区）；10 亿行 ≈ 6 万成员/册（~6MB，触碰 5 万成员阈值）；16 亿行+ ≈ 10 万成员/册（~10MB，突破 8MB slot 迁移红线）。**超限时功能仍正确**（`ZCOUNT`/`ZRANGEBYSCORE LIMIT`/`ZRANGE` 分页均为 O(log N + 批大小)），代价是 slot 迁移整体搬运破坏 8MB 有界性承诺。对策：登记册按 pk 散列细分 `exp:{table}:{stag}#{n}`——分片键机制（keycodec）保留，**但不设 DDL 声明字段**（docs/01 §1.0：不让用户申报预期行数）：细分由 Controller 依登记册体积自动触发（自治后续项，ROADMAP 在案）；在此之前，10 亿行+ 表触碰 8MB 红线的风险如实声明于此。Sweeper 逐分片轮询、COUNT(*) 逐分片汇总、全表遍历逐分片分页，三者天然容忍分片。

## 7.3 Sweeper（分布式过期清扫）

> TiDB 参照：`ttlworker` 把扫描切成 range task 分摊到多节点、心跳续租。KiDB 同构：slot 区间 = task，锁 = 租约。
> 有意不抄的部分：TiDB TTL 把 task 进度落表断点续扫——KiDB **不需要任务表**：清扫天然幂等
> （`ZRANGEBYSCORE exp -inf (now` 重新发现即重扫），进度就是数据本身。少一套机制，少一类 bug。

- **分工**：各实例按 slot 区间分摊（`lk:sweep:{slot区间}` 锁 `SET token NX PX` + `lock_renew.lua` 心跳续期，宕机自动重分配）；以 slot 编号为准而非节点，天然兼容任何二级分片实现（[03](03-数据模型与编码.md) §3.2）；
- **周期**：1s（积压自适应提速，空闲降频）。每 tick：
  ```
  ZRANGEBYSCORE exp:{table}:{stag} -inf (now LIMIT 0 512   -- 到期 pk 批
  → pipeline HGETALL rcpt:{table}:{pk}                      -- 取回执
  → 先按回执 DEL 唯一预约 key（异 slot，独立执行，自愈兜底）
  → 按 slot 分组执行 sweep_batch.lua（同 slot 原子）：
      按回执 ZREM 各索引桶条目 → ZREM exp → DEL rcpt
  ```
- **背压**：单 slot 每周期批数可配（默认 4）；积压指标 `sweeper_lag_rows` 超阈值告警并提速；
- **正确性不依赖 Sweeper 在线**：全挂时查询结果仍正确（回表空 Hash 拦截），仅内存回收延迟；恢复后积压自动追平；
- **加速通道（默认关闭）**：keyspace notification（`notify-keyspace-events Ex`）在集群模式下订阅无法经单连接覆盖全集群事件流，需逐节点直连订阅——复杂度不值，**默认不启用**，正确性无损（[09](09-后端契约与适配器.md) §9.2）。

## 7.4 全表遍历的官方替代机制（exp 登记册驱动）

> 这是禁 SCAN 后无索引谓词兜底、维表加载、对账抽样、DDL 在线回填的统一基础机制。

```
FullScan(table, filter) RowIter:
  for slot in 0..16383:                       # 经逻辑 pipeline 按节点聚合
    for shard in 0..exp_shards-1:             # 登记册细分（§7.2）
      cursor = 0
      loop:
          pks = ZRANGE exp:{table}:{stag(slot)}#{shard} cursor cursor+511
          if empty(pks): break
          rows = pipeline HGETALL d:{table}:{pk} for pk in pks   # 同 slot 批
          for row in rows:
              if row empty: continue           # 已过期/已删
              if filter(row): yield row
          cursor += len(pks)
          if ctx cancelled: return
```

对比 `SCAN MATCH`：

| 维度 | SCAN MATCH（不采用） | exp 登记册遍历 |
|---|---|---|
| 平台白名单 | 禁用 | `ZRANGE`，基础命令 |
| 路由 | 无 key → 集群客户端只能落单节点（甚至随机），数据不全 | 每命令带 key，精确路由（契约 R2） |
| 单命令耗时 | 游标步长不可控 | 固定 512，严格有界 |
| 冗余工作 | 扫全 keyspace（含其他 key 与已过期行 key） | 只扫本表名册 |
| 正确性依赖 | 无 | 依赖写入 Lua 登记的完备性（PBT P2 不变式单独断言，[12](12-测试方案.md) §12.3） |

**访问控制（v6.0：引擎层全扫闸门）**：全表遍历默认不开放——引擎层全扫（无可用索引）时按表裁决：

1. **小表自动放行**：实时行数 < `gateway.dimension_max_rows`（tuning.toml，默认 10 万）——小表全扫与维表广播同源，自动即可；
2. **表白名单放行并告警**：`query_allow_fullscan_tables`（`SET GLOBAL ... = 't1,t2'`，[10](10-配置与可观测.md) §10.2）——放行计入 `fullscan_fallback_total` 并告警日志；
3. 否则报错 `ERR_NO_INDEX`（附建索引/白名单建议）。

v6.0 起不再有 `/*+ FULLSCAN */` hint 通道（网关不解析 SQL 文本——hint 需经解析识别，与单引擎纪律冲突；逃生门保留白名单）。大表全扫仍走限流通道（exec 全扫并发信号量，tuning.toml `exec.fullscan_concurrency` 默认 10/实例，超限排队、ctx 取消贯穿）。

**16384 扇出优化**：按 slot→节点归并后经逻辑 pipeline 发出。登记册分片数 `#{n}`（§7.2）与 slot 数正交：遍历时扇出 = 16384 × n，大表细分的代价是扇出同倍放大，由限流通道吸收。

## 7.5 批量过期风暴防护

整批同时过期（如 100 万行同 TTL）场景：

- Sweeper 每 tick 批数有上限，清扫波次自然平滑；
- 单批清扫 Lua ≤512 回执、逐 slot 原子，单批 < 5ms；
- 行 key 的物理过期由 Redis 自己完成（惰性+定期），不经过 Sweeper；Sweeper 只清索引与登记册；
- 验收标准见 [12](12-测试方案.md) §12.6：100 万行同 TTL 过期，主线程无 >100ms 卡顿。
