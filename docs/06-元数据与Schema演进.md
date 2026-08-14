# 06 · 元数据与 Schema 演进

> **TiDB 参照**：schema 存 PD/etcd，全局版本号 + lease（`pkg/domain` 的 SchemaValidator）+ DDL owner 作业队列。
> KiDB 全部对齐，载体换成 Redis 自身：Catalog 存 Redis、全局版本 `ver:schema`、lease 纪律照抄、DDL 作业化。
> 缓存定位的简化：不做 TiDB 的多版本 schema 并存（那是为在线 DDL 期间跨版本事务服务的——我们没有事务，不要这套）。

## 6.1 元数据存储布局

| Key | 类型 | 内容 |
|---|---|---|
| `c:table:{table}` | Hash | 表定义：列（名/类型/默认值）、主键、索引定义（含 COMMENT 载体解析出的 KiDB 选项）、`default_ttl`、`max_row_bytes`、`expected_rows`、`exp_shards`、`dimension` 标记；`_ver`（表级版本，INCR）、`_job`（进行中的 DDL 作业，无则空） |
| `ver:schema` | String | 全局 schema 版本：任何 DDL 完成一步即 INCR——plan cache 失效锚点与 lease 刷新信号 |
| `bm:{table}:{idx}` | Hash | BucketMap（稀疏存储，[03](03-数据模型与编码.md) §3.1）+ `version` 字段 |
| `cfg:global` | Hash | 集群配置（[10](10-配置与可观测.md) §10.2）——与元数据共用同一套版本校验刷新循环 |

表级 DDL payload 字段全集（CREATE TABLE COMMENT `kidb:{...}`）：`default_ttl`（秒，0=无）、`max_row_bytes`（默认 1MB，硬上限 4MB）、`expected_rows`（量级声明，驱动 exp 分片预设）、`exp_shards`（默认 1，>10 亿行表 ≥16）、`dimension`（维表标记）。索引级 payload：`covering`（数组）、`async`（bool，唯一索引互斥）、`prefix_copy`（字典序副本）。

## 6.2 Schema lease 纪律（移植 TiDB domain/SchemaValidator）

**问题**：N 个网关实例各缓存 Catalog/BucketMap 快照，DDL 变更后如何保证 (a) 变更能快速传播，(b) 过期快照不会造成错误？

机制（与 TiDB lease 同构，按缓存定位放宽）：

1. **全局锚点**：`ver:schema` 单调递增；每个实例内存记录本地快照版本；
2. **租约窗口**：`schema_lease_ms`（默认 1000ms）内实例**信任本地快照**直接使用——这是热路径零额外 RTT 的关键；
3. **越界必检**：距上次校验超过 lease 的实例，下一次元数据使用前必须先比对 `ver:schema`（一次 `GET`，经逻辑 pipeline 与业务命令合流，不占独立连接）；版本不变 → 重置租约计时；版本变 → 全量刷新 Catalog/BucketMap 快照；
4. **写路径独立兜底**：写 Lua 内对 BucketMap version 做 CAS（[05](05-写入路径.md) §5.1 第 4 步）——即使 lease 窗口内写者拿到旧桶布局，脚本内 CAS 也会返回 stale 强制刷新重试。**正确性从不依赖 lease 守约，lease 只是性能优化**（对齐 TiDB：lease 是优化，schema 版本校验才是正确性）；
5. **读路径容忍窗口**：lease 窗口内读到旧布局（如桶刚分裂），后果只是多扫一个父桶/旧桶——回表校验兜底中间态（[04](04-查询路径.md) §4.3），不会出错行；
6. **plan cache 联动**：缓存条目携带 schema/bm 版本，命中前比对（[02](02-SQL服务器.md) §2.6）——DDL 后旧计划惰性失效，无需主动广播。

与 TiDB 的差异（有意为之）：TiDB lease 违约会阻塞/失败 SQL（强 schema 一致）；KiDB lease 违约最坏走旧桶布局多扫一点、写路径 stale 重试——**缓存定位下"短暂性能退化"优于"可用性阻断"**。

## 6.3 DDL 作业流（在线建索引）

> **v1 实现状态**：CREATE INDEX 回填与 DROP 清理当前为**同步执行**（gateway 包
> ExecDDL，小数据成立且已覆盖正确性论证：Catalog 先落库→并发写入双写覆盖回填窗口→
> ZSet 幂等吸收交错）。本节的后台作业化（`_job` 持久化 + 任意实例接管续作 + 限流回填）
> 是大表路径，随桶状态机批次切换。

DDL 语句经 [02](02-SQL服务器.md) §2.4 解析校验后，不是同步完成，而是落为作业：

```
1. 校验（fail-fast）：类型白名单 / 索引数≤16 / 列数≤256 / `_` 前缀列拒绝 /
   covering 必须同步 / async 与 unique 互斥 / int64 范围索引 2^53 检查
2. 作业落库：Catalog HSET `_job` = {type, 目标定义, 状态, 进度游标, 起始时间}（带表级 _ver CAS）
3. 执行（按类型）：
   - CREATE TABLE：立即生效（空表无回填）
   - CREATE INDEX：表标记 index_building → 后台回填：exp 登记册遍历分批取 pk（[07](07-TTL与过期清扫.md) §7.4）
     → pipeline 回表取字段 → 按批 ZADD 进新桶（批 ≤512，限流通道，默认 1 万行/s/实例）
     → 回填期间的新写入：写 Lua 发现 `_job` 存在该索引 → 双写（新桶已入 Catalog 索引集，状态机按 DRAINING 语义只写新桶）
     → 追平校验：回填游标到尾 + 增量日志（如有 async）清空 → 作业完成
   - DROP INDEX：Catalog 移除定义（读路径即时停用）→ 后台按桶 key 规则逐批 UNLINK 清理
   - DROP TABLE：表标记 dropped（拒绝新读写，报错表不存在）→ 后台经 exp 登记册遍历逐批清理行/索引/回执 → 删除 Catalog
4. 完成：`ver:schema` INCR + `_job` 清空 + 表 `_ver` INCR → 全实例经 lease 机制秒级收敛
```

**断点续作**：作业状态持久在 Catalog `_job` 字段；执行者宕机后，任意网关实例的 Controller 在例行巡检中发现未完成 `_job` 即接管续作（无owner 独占——DDL 作业幂等，并发接管由表级 `_ver` CAS 防重）。回填全程限流 + 批有界，对在线读写的影响有上限（[12](12-测试方案.md) §12.6 验收）。

## 6.4 版本兼容与演进纪律

- Catalog/BucketMap/cfg 的 blob 编码内嵌格式版本号，前后兼容一个发布周期；
- Lua 资产按 `@version` 隔离（EVALSHA 哈希天然隔离新旧），回滚安全（[05](05-写入路径.md) §5.7）；
- 新旧内核混跑窗口内：旧实例读到新格式版本号 > 自己认识的 → 拒绝服务并报错（fail-fast，不静默误读）；发布顺序约定先升全部网关再启用新 DDL 能力（[11](11-部署与运维.md) §11.4）；
- 影子流量与抽样对账作为重大元数据格式变更的上线门禁（[12](12-测试方案.md) §12.8）。
