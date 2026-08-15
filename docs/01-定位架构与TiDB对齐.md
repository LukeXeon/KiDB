# 01 · 定位、架构与 TiDB 对齐

## 1.1 做什么

- **MySQL 协议/语法的单表 SQL**，以网关形态交付（任意语言/驱动/GUI 直连）：
  - SELECT：等值 / 范围 / AND / OR / ORDER BY / LIMIT / OFFSET / COUNT / GROUP BY / MIN / MAX / 前缀 LIKE / TTL 自省（见 [04](04-查询路径.md)）；
  - INSERT / UPDATE / DELETE / UPSERT（见 [05](05-写入路径.md)）；
  - DDL：CREATE/DROP TABLE、CREATE/DROP INDEX、ALTER TABLE 受限子集，TiDB `pkg/parser` 解析（见 [02](02-SQL服务器.md) §2.4）；
- 兼容性：AUTO_INCREMENT、Prepared Statements、客户端工具握手应答（见 [02](02-SQL服务器.md) §2.10）；
- 二级索引：同步（默认）与异步（写热点字段）两种模式，在线构建；
- 行级 TTL：过期 = 行不存在，查询表现为查不到，不报错；
- 大 key / 热 key 自动治理：桶在线分裂合并、热桶值复制（见 [08](08-自治治理与热Key.md)）；
- 集群透明：Redis Cluster rebalance 对方案无影响（见 [11](11-部署与运维.md) §11.1）；
- **部署形态：MySQL 协议网关单形态**（server-only，对标 TiDB 交付形态；决策记录见 [11](11-部署与运维.md) §11.2）；
- **后端可替换**：内核只依赖 `KvClient` 抽象接口，附 go-redis/v9 参考适配器与适配器一致性测试套件。

## 1.2 不做什么（边界声明）

| 不支持 | 原因 | 行为 |
|---|---|---|
| 跨 slot 多行事务（`START TRANSACTION`） | 集群模式下 `MULTI/EXEC` 类无 key 命令无法保证路由一致性（[09](09-后端契约与适配器.md) §9.5）；方案原子性全部依赖单 slot Lua | 明确报错 |
| 大表任意 JOIN（两张大表非主键等值关联） | 无界操作 | 报错引导回数据库；有界 JOIN 三档支持（[04](04-查询路径.md) §4.4） |
| 全文检索、GEOSEARCH、流式聚合 | 超出缓存查询层定位 | 报错 |
| 强一致读 | 异步索引与读副本路径为最终一致（秒级） | 回表校验保证不出错行 |
| TRUNCATE TABLE | 全表删除须经全表遍历逐行清扫，代价无界；缓存语义下用 TTL 或重建表替代 | 报错（[14](14-红线局限与检查单.md)） |
| GRANT/REVOKE 权限体系 | v5.0 边界：账号在引导配置声明，只读/读写两级 | 报错（[02](02-SQL服务器.md) §2.9） |
| 物化视图、RESP3 client tracking、预测式热点复制 | 超出可维护性红线（见 [14](14-红线局限与检查单.md)） | 不实现 |
| keyspace `SCAN` / `KEYS` 全量枚举 | 平台禁用；且集群模式下 keyspace 扫描必须逐节点扇出，客户端单命令语义不成立 | **方案零依赖**，替代机制见 [07](07-TTL与过期清扫.md) §7.4 |
| 不支持 EVAL 的 Redis 后端 | EVAL 是写入原子性的唯一机制 | 启动期能力探测，缺失则拒绝启动 |

## 1.3 与 TiDB 的同构关系（方向正确性的证据）

KiDB 与 TiDB 在架构形状上同构——**无状态 SQL 计算层 + 共享 KV + 版本化元数据 + 选主的后台作业**。逐项映射：

| TiDB / TiKV | KiDB | 同构点 |
|---|---|---|
| TiDB server（无状态 SQL 层） | 网关 + 内核（共享状态全在 Redis 元数据） | 计算层无状态，任意扩缩容 |
| TiKV Region（96MB 自动分裂/合并） | 索引桶（8MB/5 万成员，在线分裂合并） | 同一思想；KiDB 在应用层实现 TiKV 在存储层做的事（[08](08-自治治理与热Key.md)） |
| PD 的 region 路由表 | BucketMap（本地缓存 + version 校验） | 路由元数据版本化（[06](06-元数据与Schema演进.md)） |
| `tablecodec`：`t{tableID}_r{handle}` 行、`t{tableID}_i{indexID}{vals}` 索引 | `d:{table}:{pk}` 行、`i:{table}:{idx}…` 桶；单点 `keycodec` 包纪律 | key 布局唯一所有者（[03](03-数据模型与编码.md) §3.1） |
| MVCC + Percolator 2PC（跨 Region 原子） | 单 slot Lua + hash tag 内聚 | 原子性机制（根本分野，见 §1.4） |
| 唯一索引 key（值→handle，commit 冲突检测） | 唯一预约 key `SET NX`（[05](05-写入路径.md) §5.3） | 唯一性判定按值散列；TiDB 方案的 Redis 化 |
| Coprocessor DAG 下推 | `pushdown_filter.lua` 谓词下推 | 计算向存储移动（[04](04-查询路径.md) §4.2） |
| `distsql` `kv.Request`（KeyRanges/Concurrency/流式） | `planner.Request` 对象 + scatter-gather | 扇出模型（[04](04-查询路径.md) §4.3） |
| `owner` 选举（etcd session + watchdog） | `lk:ctrl` 锁选举 + watchdog 闭环 | 后台作业选主（[08](08-自治治理与热Key.md) §8.5） |
| TTL 作业（`pkg/ttl`，SplitScanRanges 分片扫） | Sweeper（slot 区间分摊 + 锁 + 批） | 过期清扫分工（[07](07-TTL与过期清扫.md) §7.3） |
| schema lease + 版本校验（`pkg/domain`） | Catalog/BucketMap `_ver` + lease 纪律 | 元数据演进协议（[06](06-元数据与Schema演进.md) §6.2） |
| `SHARD_ROW_ID_BITS`/`AUTO_RANDOM` 打散写热点 | 异步索引 pk 全局散列 + `ver`/`seq` 分片 | 写热点摊平（[05](05-写入路径.md) §5.2、[08](08-自治治理与热Key.md) §8.6） |
| `mysql.*` 系统表 / `SHOW VARIABLES` | `cfg:global` 内置系统表 + `SET GLOBAL` | 配置即数据（[10](10-配置与可观测.md) §10.2） |
| plan cache（schema 版本绑定失效） | plan cache（fingerprint + schema/bm 版本绑定） | 计划缓存失效纪律（[02](02-SQL服务器.md) §2.6） |

**结论**：方向正确。差异全部来自 KV 层能力差（§1.4），不是设计偏差。每个机制章开头都有 "TiDB 参照 → Redis 约束 → KiDB 设计" 的推导块；三条整体路线（直接用 TiDB / 搬 SQL 层伪装 TiKV / 翻译式复用）的评估与尸检见 [13](13-TiDB复用清单.md)。

## 1.4 根本分野：KV 层能力差决定的设计后果

| TiKV 有、Redis 没有 | KiDB 的对应设计 |
|---|---|
| 跨 Region 分布式事务（2PC） | 原子性收敛到**单 slot Lua**；行与索引桶/回执/登记册同 slot（hash tag）；跨 slot 唯一性走预约 key 两阶段（已知窗口如实声明，[05](05-写入路径.md) §5.3） |
| MVCC / 快照读 | 无快照；最终一致窗口由回表校验兜底（结果精确，[04](04-查询路径.md) §4.3） |
| 存储层内建 Region 分裂/合并 | 应用层桶状态机 + Controller 控制循环（[08](08-自治治理与热Key.md) §8.3） |
| Coprocessor 富计算下推 | 白名单谓词形态的参数化 Lua 下推（[04](04-查询路径.md) §4.2） |
| 强一致（Raft） | 缓存语义，AP + 最终一致；副本读滞后由回表校验拦截 |
| 存储层数据归自己管（raft/搬迁/副本） | 数据分布、failover、复制全部交给既有的 Redis Cluster——KiDB 是查询/治理层，不是数据库 |

**这也是为什么 KiDB 不能也不应变成 TiDB**：没有 raft、没有 MVCC、没有 2PC，不是缺陷而是定位——底层是别人运维的 Redis Cluster，KiDB 只做查询层与自治治理。

## 1.5 硬约束（来源与影响）

约束的完整论证见 [09-后端契约与适配器.md](09-后端契约与适配器.md)；此处仅列结论：

| 约束 | 对方案的影响 |
|---|---|
| Redis 单线程事件循环，单命令耗时有界 | 所有范围命令强制 LIMIT/分页；单桶成员 < 5 万 |
| 平台禁用 keyspace `SCAN` | 等值桶 ZSet 化（ZRANGE 分页）；全表兜底走 exp 登记册（[07](07-TTL与过期清扫.md)） |
| 多 key 命令/Lua 限单 slot（CROSSSLOT） | 行与其索引桶、回执、计数器同 slot（hash tag）；跨 slot 只做客户端并发单 key 命令 |
| 无 key 命令路由不可靠（集群客户端普遍按随机/固定节点处理） | 方案所有命令必须携带 key；禁用 `MULTI/EXEC/WATCH`；EVAL 必须 `numkeys≥1` 且首 key 带 hash tag |
| Set/ZSet 成员无 TTL | 索引过期清理靠 Sweeper + 过期回执，无捷径 |
| slot 迁移按 key 整体搬运 | 单桶体积 < 8MB，rebalance 代价有界 |
| 无服务端模块假设 | 一切索引、统计、清扫逻辑在网关实现；模块命令不进核心路径 |

## 1.6 总体架构与包布局

```
业务（任意 MySQL 客户端/驱动/GUI）
        │  MySQL wire protocol
        ▼
┌─ KiDB 网关（cmd/kidb-server）────────────────────────┐
│  gateway   协议层：前置分类器 / 会话注册表 / 握手兼容    │
│  ddl       DDL 执行器（TiDB parser AST → Catalog 作业）│
│  planner   谓词翻译 / Request / plan cache / scatter   │
│  exec      gather / 回表校验 / RowIter 流式            │
│  txguard   写入 Lua 编排 / 幂等 / 唯一预约             │
│  meta      Catalog/BucketMap 缓存 + schema lease       │
│  config    cfg:global 配置存储（SET GLOBAL）           │
│  controller/sweeper/indexer/telemetry  后台角色        │
│  nearcache 进程内近缓存（分片 map + 周期清扫）         │
└─────────────── kidb（根包：Kernel/Querier/KvClient/Bootstrap）┘
        │
        ▼
KvClient（接口，契约 R1~R7）──► [adapter/goredis 参考实现]
                              [各公司私有适配器]
        │
        ▼
Redis Cluster（16384 slot；keycodec 单点负责 key 布局）
script 包：Lua 资产 embed + 启动期静态校验
```

**分层纪律**：

- 内核只 import 标准库与开源依赖，**不出现任何公司私有包**；
- `Client` 接口与能力探测（Capabilities）是内核与具体 Redis 客户端之间的唯一契约（[09](09-后端契约与适配器.md) §9.3）；
- key 布局的唯一所有者是 `keycodec` 包（对齐 TiDB `tablecodec` 纪律），任何包不得手工拼接 key 字符串；
- 适配器一致性测试套件（contract tests）保证任何新适配器满足契约（[12](12-测试方案.md) §12.4）。

内核只暴露：

```go
type Querier interface {
    Query(ctx context.Context, query string) (sql.RowIter, error)
    Exec(ctx context.Context, query string) (sql.OkResult, error)
}
```

产品形态只有网关一种；`Querier` 同时作为测试与带外工具的程序化入口（工程接缝，非第二产品形态——正如 TiDB 可用 mockstore 进程内起实例跑测试，但那不是产品形态）。

## 1.7 正确性与性能的总原则

1. **结果必须精确，统计可以近似**：一切返回给用户的行经过回表校验；COUNT(*) 任意时刻精确（exp 登记册 ZCOUNT 汇总）；近似仅用于优化器决策与分裂判断（HLL、1/64 采样），误差有界且文档化。
2. **一切命令有界**：任何单命令耗时 < 几 ms（桶 < 5 万成员 / < 8MB，范围查询带 LIMIT，批量命令批 512）；任何查询不物化全量结果（RowIter 全链路流式）。
3. **故障安全**：所有自治机制（Sweeper/Controller/Indexer/L4 副本）全挂时，系统只会变慢/变浪费，**不会出错行**——正确性由写入 Lua 的原子性与回表校验独立保证。
