# 01 · 定位、架构与 TiDB 对齐

## 1.0 设计原点（最高指导原则）

> **把简单留给用户，把复杂留给自己；自动优于手动；约定优于配置。**
>
> 业务方只需要会 SQL——连上的是一个"长得像 MySQL"的缓存。一切需要"先读文档再配参数"
> 的设计都是本方案的失败：能自动推导的（桶分裂、维表判定、前缀副本、索引统计）就不设开关；
> 必须有默认的取保守安全值；只有改变**语义**的选择（TTL、副本读、行缓存、全扫放行）
> 才暴露为配置，且全部有安全默认。运维面同理——变量表以"个位数"为纪律。

## 1.1 做什么

- **MySQL 协议/语法的单表 SQL**，以网关形态交付（任意语言/驱动/GUI 直连）：
  - SELECT：等值 / 范围 / AND / OR / ORDER BY / LIMIT / OFFSET / COUNT / GROUP BY / MIN / MAX / 前缀 LIKE / TTL 自省（见 [04](04-查询路径.md)）；
  - INSERT / UPDATE / DELETE / UPSERT（见 [05](05-写入路径.md)）；
  - DDL：CREATE/DROP TABLE、CREATE/DROP INDEX（gms 引擎托管，KiDB 扩展经 COMMENT 载体，见 [02](02-SQL服务器.md) §2.3）；
- 兼容性：AUTO_INCREMENT、Prepared Statements、客户端工具握手应答（见 [02](02-SQL服务器.md) §2.9）；
- 二级索引：同步（默认）与异步（写热点字段）两种模式，在线构建；
- 行级 TTL：过期 = 行不存在，查询表现为查不到，不报错；
- 大 key / 热 key 自动治理：桶在线分裂合并、热桶值复制（见 [08](08-自治治理与热Key.md)）；
- 集群透明：Redis Cluster rebalance 对方案无影响（见 [11](11-部署与运维.md) §11.1）；
- **部署形态：MySQL 协议网关单形态**（server-only，对标 TiDB 交付形态；决策记录见 [11](11-部署与运维.md) §11.2）；
- **后端可替换**：内核只依赖 `kv.Client` 抽象接口（`kidb/kv` 包），附 go-redis/v9 参考适配器（`kidb/kv/goredis`）与适配器一致性测试套件。

## 1.2 不做什么（边界声明）

| 不支持 | 原因 | 行为 |
|---|---|---|
| 跨 slot 多行事务（`START TRANSACTION`） | 集群模式下 `MULTI/EXEC` 类无 key 命令无法保证路由一致性（[09](09-后端契约与适配器.md) §9.5）；方案原子性全部依赖单 slot Lua | 明确报错 |
| 大表任意 JOIN（两张大表非主键等值关联） | 无界操作 | 报错引导回数据库；有界 JOIN 三档支持（[04](04-查询路径.md) §4.4） |
| 全文检索、GEOSEARCH、流式聚合 | 超出缓存查询层定位 | 报错 |
| 强一致读（跨结构原子同见） | v7.0 一致性级别 = **Redis 级**（§1.8）：正常路径写 OK 即可见；故障路径允许有界收敛窗口（缓存 miss 语义） | 回表校验保证查到的必对；窗口经对账/补写收敛（[14](14-红线局限与检查单.md)） |
| TRUNCATE TABLE | 全表删除须经全表遍历逐行清扫，代价无界；缓存语义下用 TTL 或重建表替代 | 报错（[14](14-红线局限与检查单.md)） |
| GRANT/REVOKE 权限体系 | 边界：账号在引导配置声明，只读/读写两级 | 报错（[02](02-SQL服务器.md) §2.9） |
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
| MVCC + Percolator 2PC（跨 Region 原子） | 行本地 Lua 原子 + 版本戳 member 两段写（v7.0） | 索引按值/索引独立寻址——**v7.0 起索引组织与 TiDB 同构**（索引 KV 独立于行位置），差异仅剩无 2PC 的收敛窗口（[05](05-写入路径.md) §5.1） |
| 唯一索引 key（值→handle，commit 冲突检测） | 唯一预约 key `SET NX`（[05](05-写入路径.md) §5.3） | 唯一性判定按值散列；TiDB 方案的 Redis 化 |
| Coprocessor DAG 下推 | `pushdown_filter.lua` 谓词下推 | 计算向存储移动（[04](04-查询路径.md) §4.2） |
| `distsql` `kv.Request`（KeyRanges/Concurrency/流式） | `planner.Request` 对象 + 桶寻址散取 | v7.0：按值/索引定位，16384 穷举扇出消亡（[04](04-查询路径.md) §4.3） |
| `owner` 选举（etcd session + watchdog） | `lk:ctrl` 锁选举 + watchdog 闭环 | 后台作业选主（[08](08-自治治理与热Key.md) §8.5） |
| TTL 作业（`pkg/ttl`，SplitScanRanges 分片扫） | Sweeper（slot 区间分摊 + 锁 + 批） | 过期清扫分工（[07](07-TTL与过期清扫.md) §7.3） |
| schema lease + 版本校验（`pkg/domain`） | Catalog/BucketMap `_ver` + lease 纪律 | 元数据演进协议（[06](06-元数据与Schema演进.md) §6.2） |
| `SHARD_ROW_ID_BITS`/`AUTO_RANDOM` 打散写热点 | 异步索引 pk 全局散列 + `ver`/`seq` 分片 | 写热点摊平（[05](05-写入路径.md) §5.2、[08](08-自治治理与热Key.md) §8.6） |
| `mysql.*` 系统表 / `SHOW VARIABLES` | `cfg:global` 内置系统表 + `SET GLOBAL` | 配置即数据（[10](10-配置与可观测.md) §10.2） |

**结论**：方向正确。差异全部来自 KV 层能力差（§1.4），不是设计偏差。每个机制章开头都有 "TiDB 参照 → Redis 约束 → KiDB 设计" 的推导块；三条整体路线（直接用 TiDB / 搬 SQL 层伪装 TiKV / 翻译式复用）的评估与尸检见 [13](13-TiDB复用清单.md)。

## 1.4 根本分野：KV 层能力差决定的设计后果

| TiKV 有、Redis 没有 | KiDB 的对应设计 |
|---|---|
| 跨 Region 分布式事务（2PC） | 原子性收敛到**行本地 Lua**（行+回执同 slot，hash tag）；索引桶/登记册按值/索引独立寻址（v7.0），跨 slot 为客户端两段 pipeline——版本戳 member 幂等，并发交错不漏（[05](05-写入路径.md) §5.1）；跨 slot 唯一性走预约 key 两阶段（已知窗口如实声明，§5.3） |
| MVCC / 快照读 | 无快照；最终一致窗口由回表校验兜底（结果精确，[04](04-查询路径.md) §4.3） |
| 存储层内建 Region 分裂/合并 | 应用层桶状态机 + Controller 控制循环（[08](08-自治治理与热Key.md) §8.3） |
| Coprocessor 富计算下推 | 白名单谓词形态的参数化 Lua 下推（[04](04-查询路径.md) §4.2） |
| 强一致（Raft） | **一致性级别 = Redis 级**（v7.0 裁决，§1.8）：单写原子可见（OK 边界），行↔索引故障窗口有界收敛；副本读滞后由回表校验拦截 |
| 存储层数据归自己管（raft/搬迁/副本） | 数据分布、failover、复制全部交给既有的 Redis Cluster——KiDB 是查询/治理层，不是数据库 |

**这也是为什么 KiDB 不能也不应变成 TiDB**：没有 raft、没有 MVCC、没有 2PC，不是缺陷而是定位——底层是别人运维的 Redis Cluster，KiDB 只做查询层与自治治理。

## 1.5 硬约束（来源与影响）

约束的完整论证见 [09-后端契约与适配器.md](09-后端契约与适配器.md)；此处仅列结论：

| 约束 | 对方案的影响 |
|---|---|
| Redis 单线程事件循环，单命令耗时有界 | 所有范围命令强制 LIMIT/分页；单桶成员 < 5 万 |
| 平台禁用 keyspace `SCAN` | 等值桶 ZSet 化（ZRANGE 分页）；全表兜底走 exp 登记册（[07](07-TTL与过期清扫.md)） |
| 多 key 命令/Lua 限单 slot（CROSSSLOT） | 行与回执同 slot（hash tag）；索引桶/登记册经独立寻址后**不再受此约束**（v7.0）——跨 slot 只做客户端并发单 key 命令 |
| 无 key 命令路由不可靠（集群客户端普遍按随机/固定节点处理） | 方案所有命令必须携带 key；禁用 `MULTI/EXEC/WATCH`；EVAL 必须 `numkeys≥1` 且首 key 带 hash tag |
| Set/ZSet 成员无 TTL | 索引过期清理靠 Sweeper + 过期回执，无捷径 |
| slot 迁移按 key 整体搬运 | 单桶体积 < 8MB，rebalance 代价有界 |
| 无服务端模块假设 | 一切索引、统计、清扫逻辑在网关实现；模块命令不进核心路径 |

## 1.6 总体架构与包布局

```
业务（任意 MySQL 客户端/驱动/GUI）
        │  MySQL wire protocol
        ▼
┌─ go-mysql-server（协议 + 解析 + 分析器 + 执行器框架）─────────────┐
│  engine    gms 扩展点实现：Provider/Database（TableCreator/        │
│            TableDropper）/Table（扫描、索引、编辑、投影、统计）/    │
│            Session（事务显式拒绝、角色）/DDL 转换校验              │
│  exec      谓词翻译执行：散取/k 路归并/回表校验/RowIter 流式        │
│  txguard   写入 Lua 编排 / 幂等 / 唯一预约 / HLL 采样             │
│  meta      Catalog/BucketMap 缓存 + schema lease                   │
│  config    cfg:global 配置存储（3 个语义开关的持久化）             │
│  controller/sweeper/indexer/telemetry  后台角色                    │
│  nearcache 进程内近缓存（otter 底座）                              │
│  tuning    开发者调优参数（tuning.toml embed，唯一调优面）         │
└─────────────── kidb/kv（Client 契约/退避/SyncClock）+ kidb 根包（Bootstrap/错误码）─┘
        │
        ▼
kv.Client（接口，契约 R1~R7）──► [kv/goredis 参考实现]
                              [各公司私有适配器]
        │
        ▼
Redis Cluster（16384 slot；keycodec 单点负责 key 布局）
script 包：Lua 资产 embed + 启动期静态校验
gateway 包：纯装配（引擎构造 + wire server + 账号/角色/变量注册 + 后台角色），
            **不含任何 SQL 文本处理**
cmd/kidb-server：进程入口（DI 装配，wire）
```

**分层纪律**：

- 内核只 import 标准库与开源依赖，**不出现任何公司私有包**；
- `kv.Client` 接口与能力探测（Capabilities）是内核与具体 Redis 客户端之间的唯一契约（[09](09-后端契约与适配器.md) §9.3）；
- key 布局的唯一所有者是 `keycodec` 包（对齐 TiDB `tablecodec` 纪律），任何包不得手工拼接 key 字符串；
- 适配器一致性测试套件（contract tests）保证任何新适配器满足契约（[12](12-测试方案.md) §12.4）；
- **v6.0 新纪律**：网关/装配层不做 SQL 文本解析；分析面与执行面同一语法树（gms）。
  独立进程非库——不使用 `internal/` 可见性约束（包即边界，tuning/testutil 平铺）。

内核对外暴露 = **gms 引擎**（装配层直挂 `sqle.Engine`，DI 图唯一入口）。

产品形态只有网关一种。~~`Querier` 程序化接口~~（v6.x review 删除：零实现零消费的
文档时代遗骸——v6.0 后 SQL 入口就是 gms 引擎，内核不再设第二 SQL 接口；
测试与带外工具同样经引擎或经 engine.Deps 直接装配）。

## 1.7 正确性与性能的总原则

1. **结果必须精确，统计可以近似**：一切返回给用户的行经过回表校验；COUNT(*) 任意时刻精确（集中登记册 ZCOUNT——v7.0 起 16384 册 → 单册，精确性从不依赖扇出宽度）；近似仅用于优化器决策与分裂判断（HLL、1/64 采样），误差有界且文档化。
2. **一切命令有界**：任何单命令耗时 < 几 ms（桶 < 5 万成员 / < 8MB，范围查询带 LIMIT，批量命令批 512）；任何查询不物化全量结果（RowIter 全链路流式）。
3. **故障安全**：所有自治机制（Sweeper/Controller/Indexer/L4 副本）全挂时，系统只会变慢/变浪费，**不会出错行**——正确性由行本地 Lua、版本戳 member 幂等与回表校验独立保证；**可见性**（新写入索引项的即时可见）在故障路径允许有界延迟（v7.0 声明，§1.8）。

## 1.8 一致性规格（v7.0 裁决：级别 = Redis 级）

| 维度 | Redis 本体的级别 | KiDB v7.0 承诺 |
|---|---|---|
| 单 key 写后读 | 写成功即对所有客户端可见 | 写入回 OK 后，点查与索引查询对任何客户端可见（正常路径零窗口：行 Lua 与索引命令同 pipeline，全部成才回 OK） |
| 跨 key 原子性 | **无**（multi-key 限同 slot，无跨 slot 事务） | 行 ↔ 其索引项**不要求**原子同见；故障路径（崩溃/断连于第二段）允许有界收敛窗口，以缓存 miss 形式呈现，异步补写 + 对账收敛（[05](05-写入路径.md) §5.1、[12](12-测试方案.md)） |
| 副本读 | 异步复制窗口，可能读旧 | L3 副本读 / L4 热桶副本同性质窗口（[08](08-自治治理与热Key.md)） |
| 故障表现 | 主切换丢尾部写 = key 丢失 | 行成索引败 = 该值索引查不到 = 缓存 miss 语义；反向（索引在、行无）由回表校验滤除 |

两个维度必须分清：**精确性**（查到的必对——回表校验，永不放宽）与**及时性**（写完立即可见——正常路径零窗口，故障路径有界）。v7.0 放的是后者中的故障路径，前者是全部安全网的根基。

