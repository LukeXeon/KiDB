# Changelog

所有显著的变更、修正与设计裁决记录于此。docs/ 内只保留当前设计与实现状态，不写历史。

## v5.1（实现期，进行中）

### 落地（docs 全册对应的可运行系统）

- **SQL 服务器端到端**：go-mysql-server 引擎绑定（Table/Index/编辑器/统计全套接口）、MySQL 协议网关、认证（MySQLDb）+ ro/rw 两级执法、预处理语句；
- **DDL 路径**：TiDB `pkg/parser` 解析（独立 module 直接依赖、零 fork），KiDB 扩展经 `COMMENT 'kidb:{json}'` 载体；在线建索引作业化（`_job` 游标、Controller 巡检接管、Building 标记完成前查询不可见）；
- **写入路径**：`write_row.lua` 单 slot 原子提交（撤旧索引/写行/建新索引/登记过期/回执/计数），唯一约束预约 key 两阶段，复活语义，CAS 写语义；
- **查询路径**：流式 scatter-gather、回表校验（一切异步路径兜底）、谓词下推 Lua（P4 双实现比对）、L1 近缓存（otter 底座）；
- **自治链路**：遥测采样 → 候选登记 → 精确复核 → 桶分裂/合并全协议（断点续作）+ L4 热桶副本生命周期 + 锁选举 watchdog 闭环（续约失败立即卸任）；
- **TTL 清扫**：分布式 Sweeper（复活复查跨脚本不变式）；
- **配置即数据**：`cfg:global` + CAS + 变量表校验，网关拦截 SET/SHOW GLOBAL；
- **指标体系**：prometheus 全系列 + 关键钩子；**退避矩阵**：错误分类（CLUSTERDOWN/LOADING/READONLY/TRYAGAIN…）按类分派，耗尽映射 1105；
- **契约一致性套件**：单节点全 slot 真实集群（docker），R1~R7 + CROSSSLOT 断言；
- **PBT**：分类器对拍（P5）、DDL 作业（P6）、分裂/合并状态机（P1）。

### 修正与裁决（实现期发现，文档相应处已更新为当前形态）

- **L4 副本 key**：从"源桶 key 后缀 `:rep{k}`"改为 **stag 步进替换** `SlotTag((源slot + k×1820) % 16384)`——后缀不改变 hash tag，副本会与源桶同 slot，L4 失效；
- **BucketMap key**：从全局 `bm:{table}:{idx}` 改为**每 slot 分片**——全局 key 无法进入行写入 Lua（跨 slot 不可达，写路径版本 CAS 物理不成立）；
- **L4 `@ver` 版本戳**：改为伴生 String key——副本本体是 ZSet，单 key 单类型，"Hash 副字段"不可达；
- **范围分裂选边**：按子桶完整区间记录（分裂点是采样中位数，非算术中点）；
- **GROUP BY COUNT 的 ZCARD 直推不采用**：过期未清扫成员会虚增计数，破坏"结果必须精确"纪律；该形态由引擎层聚合承载；
- **覆盖列 member**：改 msgp 数组编码——原竖线拼接在 pk/值含分隔符时截错；
- **异步日志条目**：字段转义消歧（`\x1f` 分隔符风险）；
- **异步索引桶布局**：与同步索引一致（行 slot 内聚）——初稿的"pk 全局散列异 slot 桶"是不必要的寻址分叉，写热度摊平的真实来源是行 slot 按 pk CRC16 散列；
- **stag 表初始化**：coupon-collector 单趟分配（逐 slot 定向扫描是 O(N²)，启动慢两个数量级）；
- **COMMENT 载体语法**：统一空格分隔形式（`COMMENT 'kidb:...'`——TiDB parser 不收 `COMMENT='...'` 无空格等号形式）；
- **DDL 时钟纪律**：miniredis 的 TIME 不随 FastForward 走（只推进 TTL 钟）——TTL 测试注入共享可推进钟；
- **go-redis 泛化 Do 回复形态**：Hash 类回复为 map[any]any，适配器归一为 map[string]string；
- **L1 近缓存底座**：换 `maypok86/otter/v2`（读路径过期判定实证满足"过期值不返回"纪律；自研分片 map 弃用——维护面 -1）；
- **接口命名统一**：`kidb.Client` → `kidb.Store` → `kidb.KvClient`；
- **编码切换 msgp**（tinylib/msgp 代码生成）：Catalog `def`/`_job`、BucketMap 条目；`_fmtv` seam 升 2；
- **快速路径**：COUNT(*) 全表（exp ZCOUNT 汇总）与 MIN/MAX（端点归并 + 回表校验跳脏）；TiDB parser 在 DML 侧仅限识别性使用。

### B 组性能增强（按 ROADMAP 序推进）

- **top-k 归并下推 + ORDER BY 排序正确性修复**：探针复现并修复潜在乱序——gms `replace_sort.go` 在 ORDER BY 列与索引前缀匹配时直接删除 Sort 节点（ASC 不咨询任何接口），而 KiDB 范围桶按 slot 散布、原 slot 组流式产出全局无序。修复 = RangeLookup 一律走 k 路归并（exec/topk.go：16384 路种子建堆 + 按需补页，`gopkg.in/dnaeon/go-priorityqueue.v1` 泛型堆；DESC 走 `ZREVRANGEBYSCORE` + 大顶堆），排序正确性由构造保证；`Index` 实现 `sql.OrderedIndex`（范围=Asc/Reversible，其余=None）+ `CanSupport` 收紧（等值/唯一/主键仅点范围）为契约防御面；LIMIT 早停由引擎停止消费自然达成，keyset 分页（`WHERE num > ? ORDER BY num LIMIT k`）随之成为最优路径。副带修复：sqlguard 收集 IN/BETWEEN 谓词列（`WHERE pk IN (...)` 曾被误拒 ERR_NO_INDEX）、ORDER BY 范围索引列的无 WHERE 查询按有界放行。
- **投影下推 + 覆盖索引读路径**：`Table` 实现 gms `ProjectedTable`——回表从 HGETALL 降为 HMGET 列子集（零宽投影退化为 EXISTS 活性判定）；覆盖索引命中（投影∪谓词 ⊆ 索引列+覆盖列+pk）时**跳过回表**：member msgp 解码覆盖列 + `ZSCORE exp` 活性校验（过期行绝不返回），解码失败防御性回退回表。配套修复：在线回填（`_job`）补上 member 覆盖列编码（此前回填产裸 pk，覆盖读路径对回填索引全灭）；DDL 覆盖列必须 NOT NULL（msgp 字符串数组无法保真 NULL，防静默错值）。**gms 集成实证**：`PrimaryKeySchema()` 必须恒返全量 schema——投影窄 schema 会让 coster 的 `indexFds` 找不到 PRIMARY 索引列、nil stat 在 `ordinalsForStat` panic；gms `fix_exec_indexes` 显式兼容宽 PrimaryKeySchema（dolt 同形）。
- **L2 请求合并（singleflight）**：EqLookup 同指纹并发查询共享一次散取——leader 物化 pk 列表（2^20 上限，超出各调用方退回独立流式，有界性优先）并填充 L1，followers 经 `DoChan` 共享（leader 客户端断开不连坐）后各自回表校验。命令量实证：32 路并发冷查询热值，桶读取 ≈1 次散取而非 32 次。配套补上 **L1 网关装配缺口**（此前 `SetNearCache` 只在测试内接线，生产路径 L1 休眠）：装配进 `startRoles`，`nearcache_ttl/_capacity` 变量轮询换装。
- **L3 副本读进读路径**：契约新增 `PipelineReplica`（批级副本读——散取/回表是批量形态，单命令 `DoReplica` 无法满足 RTT 纪律；`KvClient` 契约面扩为六方法，docs/09 §9.3 同步）；参考适配器改为**独立 `ReadOnly` 副本客户端**（修正原实现：`ReadOnly` 设在主客户端上会把 go-redis 内建只读命令表的全部读分流到副本，主节点读纪律失守）；exec 读命令统一走 `readPipeline`/`readDo` 分流出口；开关 = `replica_read` 变量 × 适配器能力位（轮询热更，能力缺失自动降级）。cmd 新增 `--replica-read` 能力开关。
- **行级近缓存（hotkey_row_cache）**：pk→行投影近缓存落地（此前只注册了变量）——命中零 RTT；条目 TTL = min(默认 TTL, 行剩余 PTTL)（HGETALL+PTTL 同 pipeline 填充，零额外 RTT），**行物理过期则条目同步死亡**；更新/删除在 TTL 窗口内返回陈旧内容（无跨实例失效广播——默认关闭的根因，docs/08 §8.4 语义如实改写，替代原文稿过于乐观的"天然保证"表述）。开关经 `hotkey_row_cache` 变量轮询生效。
- **前缀搜索 LIKE 'abc%' 查询路径**：字典序副本（`i:…#l{n}`，写路径本就在维护）接通读侧——`ZRANGEBYLEX [p [p+\xff` 分页 + k 路归并产出全局字典序（字符串堆；`ORDER BY` 前缀列时 gms 删 Sort 的契约与 top-k 同源）。引擎接入走 gms `IndexSearchableTable.LookupForExpressions`（**FilteredTable 在 gms v0.20 只在 bindvar 规则内被调用，非常驻分析面——实证死路**）；`CanSupport` 对 prefix_copy 索引精确放行前缀区间形态（其余非点范围仍拒绝，防无序路径被选为排序载体）。配套修复：在线回填此前只写主桶不写字典序副本（在线建的 prefix_copy 索引查询结果残缺）；sqlguard 放行常量前缀 LIKE。
- **慢查询日志**：网关 ComQuery 统一计时包装——超 `slow_query_threshold_ms`（新变量，默认 500ms）记录语句指纹（NormalizeDigest）/路由/行数/耗时；全扫放行（hint/白名单）与阈值无关强制告警；新增 `slow_queries_total{route}` 指标。配套补上 **cmd 指标装配缺口**（metrics 包此前只在测试接线）：`metrics.New(nil)` 入 exec + `-metrics-addr` 可选 /metrics HTTP 端点。
- **EXPLAIN 自定义输出**：网关接管 `EXPLAIN SELECT`（两列 item/detail 计划展示：命中路径/索引 ID/扇出估算/L1-L2 标记/守卫判定）；计划推断与执行共用同一套 AST 形态分析；接管点必须在快速路径之前（否则 `EXPLAIN SELECT COUNT(*)` 会被真执行——测试钉死）。
- **plan cache 判定缓存**：指纹（NormalizeDigest，lexer 级归一）→ 网关判定产物（fastpath 形状 + 守卫放行），条目绑定全局 schema 版本（lease 内零 RTT 比对，惰性精确失效）；全扫依赖判定不进缓存（随 query_allow_fullscan_tables 漂移）；LRU 容量 `plan_cache_capacity` 热更。顺带：fastpath/guard 双份 TiDB 解析合并为单 parse 联合评估；**补齐预处理语句执法缺口**（ComPrepare 此前绕过事务拒绝/ro/守卫直达引擎，现 PREPARE 期完成全部分类与判定；DDL 不支持预处理协议明确报错）。
- **全扫/回填限流通道**：引入 `golang.org/x/time/rate`——DDL 回填按 `ddl_backfill_rate_limit`（默认 1 万行/s/实例）经 rate.Limiter 限速（JobRunner.flush 按行申请额度）；全扫并发信号量（`query_fullscan_rate_limit` 默认 10，超限排队而非击穿集群，ctx 取消贯穿，槽位随 EOF/Close 释放）。两变量经配置轮询热更。
- **HLL 基数统计接入**：写路径按值确定性采样 PFADD（`hll:{table}:{idx}` 索引级单 key；**按值而非按 pk 采样**——按 pk 时高频值几乎必中，低基数列会被 PFCOUNT×64 回补高估 ~64 倍）；回填与增量同一采样规则；读侧 `IndexCardinality`（PFCOUNT×64）+ EXPLAIN `cardinality(approx)` 展示行。AND 自动选路接管诚实留作后续项（gms coster 均匀统计当前承载）。

## v5.0（文档全面革新，docs/ 现行版本基线）

- 更名 **KiDB**；叙述方式改为推导式（每机制章 "TiDB 参照 → Redis 约束 → KiDB 设计"）；
- TiDB 对齐工程化：`pkg/parser` 独立 module 直接依赖；设计移植清单化（schema lease、plan cache 版本绑定、owner watchdog、kv.Request 形状、Backoffer 错误分类、TTL 作业分摊、tablecodec 单点 keycodec 纪律）；"搬 TiDB SQL 层伪装 TiKV"路线尸检否决；
- SQL 服务器升格为核心章：前置分类器、双解析器分工、会话、plan cache、DDL 作业化；
- 部署形态收敛为 **MySQL 协议网关单形态**（延续 v4.3 决策）；
- 契约增补错误分类与退避矩阵；缓存定位下 TiDB 强一致机制（MVCC/2PC/快照/GC）整体不采用。

## v4.3（redisql-doc 存档时代）

- 砍掉 Go 库形态，部署收敛为网关单形态（对标 TiDB server-only 交付）；
- 命名纪律统一与近缓存过期机制定稿。

## v4.2

- **唯一约束订正为预约 key 机制**（原"写入 Lua 内 ZSCORE 目标桶判存在"与行落槽模型物理冲突：同值不同 pk 落不同槽，跨槽判定单 slot Lua 不可达）；
- 后台角色自动选举语义（锁即选举，替代手工指定）；
- Client 命名统一；CRC16 现成库与 Lua 压缩工程项。

## v4.1

- 初版完整设计（项目代号 redisql）：桶模型、写入 Lua、Sweeper、契约章、测试金字塔。
