# 13 · TiDB 复用清单（v5.0 的方法论章）

> 本章回答一个会被反复问起的问题：**"TiDB 都有现成的了，为什么不直接搬？"**
> 答案分三层：能直接依赖的走 go.mod（零修改零 fork）；搬不动代码的按设计移植；
> 整层搬不动的，给出依赖链尸检。判断的唯一标准是 Redis Cluster 的能力边界与缓存定位——
> 不是偏好。

## 13.1 三条路线的评估（从头思考的记录）

| 路线 | 做法 | 结论 |
|---|---|---|
| **X. 直接用 TiDB 产品** | 拿 TiDB+TiKV 当缓存 | ❌ 定位不符：数据已经活在公司 Redis Cluster 里，KiDB 是它的查询/治理层，不是另起一套数据库 |
| **Y. 搬 TiDB SQL 层，Redis 伪装 TiKV**（实现 `kv.Storage`/`kv.Transaction` 接口） | 复用 parser+planner+executor 全套 | ❌ 死在三个接口语义上（尸检见 §13.2）；为填洞会把桶模型/hash tag/Lua 全部重新发明一遍，还多背整个 TiDB 的代码与运维面 |
| **Z. 翻译式设计 + 模块化复用（本方案）** | 约束推导机制（[09](09-后端契约与适配器.md) 为 forcing function），模块边界允许的代码直接依赖 | ✅ 采用 |

## 13.2 路线 Y 尸检：为什么 SQL 层搬不动

TiDB 的 SQL 层不是独立的——它焊死在 TiKV 的事务模型上。`kv.Transaction` 接口隐含三个 Redis 物理上给不出的能力：

| 接口语义 | TiDB 中的使用面 | Redis Cluster 的现实 | 伪装代价 |
|---|---|---|---|
| **有序迭代**（`Seek`/range scan over keyspace） | 一切索引扫描、表扫描、coprocessor、DDL 回填、TTL 作业 | 无有序 keyspace；SCAN 被平台禁用且集群下单命令语义不成立 | 必须自建全局有序索引——即把 exp 登记册/桶模型先发明回来，藏在假 TiKV 内部 |
| **跨 key 原子提交**（txn 缓冲任意 key 集合，2PC 提交） | 一次 INSERT 写 row key + N 个索引 key（且 TiDB 故意把索引 key hash 分散到不同 region） | 多 key 原子仅限单 slot Lua | 必须改 key 编码让 row+index 同 slot（= hash tag 内聚设计），然后砍掉 2PC/pessimistic lock/async commit/lock resolver 全套子系统 |
| **MVCC / 快照读**（一切读携带 StartTs） | 读路径、schema lease 校验、GC、resolved lock | 无 MVCC；缓存定位也不需要（[01](01-定位架构与TiDB对齐.md) §1.4） | 伪造单一日志时间戳 → 连锁击穿 GC/锁解析/schema 版本假设 |

结论：路线 Y 保住的只是 parser+planner+executor，而这三件 go-mysql-server 已经以库的形式提供（[10](10-配置与可观测.md) §10.1），还附送 wire protocol 与 system variables——**"能用库的用库"已经覆盖了路线 Y 想要的全部，且零手术**。

## 13.3 直接依赖清单（go.mod import，零修改零 fork）

| 依赖 | 事实 | KiDB 用途 |
|---|---|---|
| `github.com/pingcap/tidb/pkg/parser` | **独立 Go module**（自带 go.mod，go 1.25）；外部依赖仅 pingcap/errors、pingcap/log、zap、modernc 解析栈、golang.org/x/text 等公开库——零 TiKV/etcd 耦合 | ① DDL 语句解析（AST：`CreateTableStmt`/`CreateIndexStmt`/`DropTableStmt`…）；② `parser.NormalizeDigest(sql)` 产出计划缓存指纹（normalized + digest）；③ COMMENT 选项原生解析（`TableOptionComment`、`IndexOption.Comment`）——KiDB 扩展的载体（[02](02-SQL服务器.md) §2.4） |
| （间接随 parser） | parser 模块的传递依赖 | 随 go.mod 锁版本统一管理 |

**纪律**：只 go.mod 依赖，**永不 fork parser**。fork 意味着永久背锅上游语法演进；COMMENT 载体（`kidb:{json}`）正是为了不 fork 而存在的扩展通道。

## 13.4 设计移植清单（照着重写，每项都标注为什么不能直接依赖）

| TiDB 模块 | 移植到 KiDB | 移植内容 | 不能直接依赖的原因 |
|---|---|---|---|
| `pkg/domain` SchemaValidator | [06](06-元数据与Schema演进.md) §6.2 | schema lease：租约内信任本地快照、越界必检、版本校验才是正确性（lease 只是优化） | 焊在 etcd/PD 会话与 TiDB 的全局 schema 版本协议上 |
| `pkg/planner/core/plan_cache*` | [02](02-SQL服务器.md) §2.6 | plan cache 条目绑定 schema 版本，命中前比对，惰性失效 | 依赖 TiDB 的 infoschema 与计划结构 |
| `pkg/owner/manager.go` | [08](08-自治治理与热Key.md) §8.5 | owner 语义闭环：抢锁任职 → watchdog 盯续约 → **续约失败立即退出角色** → 重新竞选 | 基于 etcd session；KiDB 用 Redis `SET NX PX` + Lua 续约，语义照抄载体自研（~30 行薄层） |
| `pkg/kv/kv.go` Request 形状 | [04](04-查询路径.md) §4.3 | 单请求对象贯穿执行器→存储：范围集 + 并发 + 保序 + 副本读 + 预算 | TiDB 的 Request 面向 coprocessor 与 region；KiDB 面向桶与 slot |
| client-go `Backoffer` | [09](09-后端契约与适配器.md) §9.6 | 按错误类型分派退避策略与上限，不一律重试 | client-go 处理的是 region 错误族；KiDB 处理 Redis 错误族（MOVED/ASK/CLUSTERDOWN/LOADING/READONLY/TRYAGAIN），分类思想照抄 |
| `pkg/ttl`（SplitScanRanges/ttlworker） | [07](07-TTL与过期清扫.md) §7.3 | 清扫任务按区间分摊 + 租约续期 | TiDB TTL 依赖有序扫描与任务表；KiDB 由 exp 登记册驱动、天然幂等免任务表（有意不抄，见该章） |
| `SHARD_ROW_ID_BITS`/`AUTO_RANDOM` | [05](05-写入路径.md) §5.2、[08](08-自治治理与热Key.md) §8.6 | 写热点打散思想：异步索引 pk 全局散列、seq 分片交错 | 这是思想不是代码 |
| `tablecodec` 纪律 | [03](03-数据模型与编码.md) §3.1 | key 布局单点所有（`keycodec` 包），全仓库禁手工拼 key | 纪律不是代码 |
| coprocessor 表达式下推白名单 | [04](04-查询路径.md) §4.2 | 只下推白名单内的简单谓词形态，复杂表达式回客户端校验 | TiDB 下推 DAG 到 TiKV 执行器；KiDB 下推参数化 Lua |
| 直方图统计 | [04](04-查询路径.md) §4.6 | 桶 score 区间边界 = 免维护等深直方图 | TiDB 直方图为任意数据分布服务；KiDB 桶边界天然就是分位数 |

## 13.5 不采用清单（缓存定位判断：强一致机制整体不采用）

> 判断依据（用户确认的定位）：KiDB 是**缓存数据库**。强一致机制解决的是"唯一事实源"问题；
> 缓存的正确性姿势是"结果精确 + 失效有界"，不是"一致性读"。

| TiDB 机制 | 不采用原因 |
|---|---|
| MVCC / 快照读 / StartTs | 无 MVCC 载体；缓存不需要一致性读——回表校验以更低成本给出"不出错行"（[04](04-查询路径.md) §4.3） |
| 2PC / 悲观锁 / async commit / lock resolver | Redis 无跨 key 事务；单 slot Lua + 预约 key 已覆盖方案所需的全部原子性（[05](05-写入路径.md)） |
| GC worker / resolved ts / safepoint | 是 MVCC 的配套，同上 |
| schema 多版本并存 | 为在线 DDL 期间的跨版本事务服务；无事务则无需多版本，单版本 + lease 即可（[06](06-元数据与Schema演进.md) §6.2） |
| 统计全家桶（CM-Sketch/TopN/反馈修正） | 桶级 ZCARD 精确值已覆盖等值选择率；点频估计列为观察项（[04](04-查询路径.md) §4.6） |
| PD 热点调度（搬 Region） | slot 搬迁是平台运维行为，KiDB 不主动迁移数据；热点用 L4 值复制摊开读（[08](08-自治治理与热Key.md) §8.4） |
| BR / dumpling / lightning（备份恢复生态） | 缓存可重建，不做备份；数据重建由业务回填或全量导入完成 |
| GRANT/REVOKE 权限体系 | 部署边界问题（账号分级 + 网络隔离），[02](02-SQL服务器.md) §2.9 |
| TiDB SQL 层整体 | §13.2 尸检 |

## 13.6 复用纪律与版本基线

- **版本基线**：`pkg/parser` 版本随本仓库 go.mod 锁定；当前基线 `v0.0.0-20260814130643-17c0dd0fe42b`（TiDB master @2026-08-14，go 1.25 模块线）；升级 = 改 go.mod + 全量回归（[12](12-测试方案.md) §12.9）；
- **fork 禁令**：任何"parser 不支持我们想要的语法"的冲动，先走 COMMENT 载体；载体真不够用再评估语法 hint（`/*+ */`），仍不够才立项——fork 是最后手段且须记录决策；
- **跟踪上游**：每季度检查 parser 上游变更（安全修复、MySQL 新语法），评估是否升基线；
- **设计移植的回访**：移植项（§13.4）在 TiDB 上游有语义更新时（如 owner 语义变化），评估是否跟进——移植的是语义，语义有源头。
