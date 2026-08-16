# Changelog

所有显著的变更、修正与设计裁决记录于此。docs/ 内只保留当前设计与实现状态，不写历史。

## v7.0（索引槽位独立化，🔨 设计与早期实施中）

> 总原则（用户裁决）：**不为兼容旧路径放弃破坏性修改与最优设计**——无过渡形态、
> 无兼容层、无新旧双跑（沿 v6.0"当新项目做"纪律）。
> 设计推导稿：[docs/v7.0-索引槽位独立化.md](docs/v7.0-索引槽位独立化.md)；
> 阶段划分与裁决记录：ROADMAP「v7.0」节。

**四触发（全部用户裁决，2026-08-16）**：

1. **一致性级别 = Redis 级**：正常路径写 OK 即全客户端可见（零窗口）；故障路径
  （主 pipeline 第二段崩溃）有界收敛、以缓存 miss 呈现；查询精确性（回表校验）不放。
  → **桶按值/索引独立寻址，与行 slot 解绑，16384 穷举扇出消亡**（等值=1+K 桶、
  范围=子桶数路、COUNT(*)=单 ZCOUNT、全扫=1~n 册）；两段写 + **版本戳 member**
  （`pk \x1f _ver`）保证并发交错不漏行（不漏方向有证明，脏 member 回表滤+对账清）；
  行 Lua 收窄为"行+回执+异步日志"原子面。
2. **角色负载自适应**：`ReadWriteOnly` 开关拆除，所有节点必须参与后台竞选；
  忙闲退避式竞选（inflight 信号、无阈值开关）+ 任职退让。
3. **非泛型容器消除**：sort 预泛型 API×8 → slices；手搓集合 24 处 → `utils.Set[K]`；
  堆已泛型化（utils.PriorityQueue）；**零新第三方依赖**（网络调研后裁决；
  golang-set/v2 记名候补；跟踪官方 container/heap/v2 提案 go#77397）；
  depguard 防回潮（sort/container/list/ring 包级禁用）。
4. **空窗兜底三件套**：①空窗上界 300s（退避竞选兜底闸）；②唯一预约 TTL 自愈
  （24h PX + 过期接管）；③补写死信队列（`dlq:idx:{table}:{idx}`）。
  不做：L1 延寿、全局 inflight 闸、单桶紧急通道、本地兜底存储（红线记录入 docs/14）。

**进度**：设计稿 + 四项开放问题裁决 ✅；阶段 6（触发三，c8a144d）✅ 全测试绿；
阶段 0（docs/01/03/04/05/06/07/08/11/12/14 正式改写，三批次）✅；
阶段 1~5、7（代码实施与性能复测）待启动。批次一改写中顺带修正三处 v6 文档漂移
（L4 @ver 残留、SELECT * 含 _ttl、dnaeon/container-heap 引用），11 篇顺带修正
Querier/TiDB parser 残留。

## v6.0（架构收敛，破坏性变更，✅ 已落地 2026-08-16）

> 总原则（用户裁决）：**gms → 我们的转换层 → Redis**——在 gms 框架内实现数据库，
> 不是把它包装一层；网关不做任何 SQL 文本解析；该删的删、该改的改，不留过渡形态。
> **v6.0 当新项目做**：复用已有实现，但零存量兼容包袱（无退回旧映射/deprecated 层/
> 双格式并存）——预发布阶段变更直接演进。
> 完整任务清单与完成判据见 ROADMAP「v6.0 架构收敛」节。

已裁决并全部落地的拆除/改造（代码与文档同步完成）：

- **网关纯装配化**：handler 包装层全灭。事务拒绝改由自定义 `engine.Session`（`TransactionSession` 全显式报错——gms 对无该接口的会话静默 no-op BEGIN，是隐性部分提交陷阱，实证）；ro 执法进引擎写入口/DDL 接口/SET GLOBAL 钩子；逐语句慢日志退役（指标 + 全扫告警承接）；
- **系统变量 gms 原生**：3 个语义开关注册为 `sql.MysqlSystemVariable`，`NotifyChanged` 经 `Session.Cfg` 持久化 `cfg:global`；配置面正则拦截删除；
- **全扫闸门引擎层**：`Table.PartitionRows` 全扫前过闸（小表自动放行/白名单放行并告警/否则 ERR_NO_INDEX）；`/*+ FULLSCAN */` hint 通道取消（识别 hint 需要解析 SQL，与单引擎纪律冲突；逃生门保留白名单）；
- **历史组件全拆**：前置分类器、双解析器分工、网关快速路径（COUNT/MIN/MAX）、自定义 EXPLAIN、plan cache 判定缓存、自写指纹归一化器；**go.mod 移除 TiDB parser（零代码依赖）**。承接面：COUNT(*) 由 gms `replaceCountStar`→`StatisticsTable`（精确 RowCount=Σ ZCOUNT）；MIN/MAX 经引擎聚合（端点加速写法 `ORDER BY col LIMIT 1`）；EXPLAIN 走 gms 原生；JOIN 分档由全扫闸门统一裁决；
- **ddl 包并入 engine**（转换/校验即 `engine/ddlconvert.go`）；
- **DI 装配**：引入 `github.com/google/wire`，全项目组件统一进 DI 图；
- **工具包收敛**：`utils/` 通用工具包——自研泛型优先队列（container/heap 封装）替代并移除 `gopkg.in/dnaeon/go-priorityqueue.v1`；同名/同体函数脚本扫描 + 归拢（`Strings`/`StringMap`/`AnySlice`/`ParseUint64`/`SleepCtx`；`contains`→stdlib `slices.Contains`；engine 手工拼 key 的 `rowKeyOf`/`seqKeyOf` 回收至 keycodec——key 布局单点所有纪律的执行面漏洞）；测试断言探针 `Probe` 与命令计数器 `CmdCounter` 入 `testutil`（此前 sweeper/txguard/exec/gateway 测试各抄一份）；`internal/` 平铺（`internal/tuning`→`tuning`、`internal/redistest`→`testutil`）；
- **i18n**：引入 `github.com/nicksnyder/go-i18n/v2`——用户面向消息全部走消息目录（en 默认 + zh）；**消息不得含技术文档引用**（docs 引用只留代码注释）；
- **泛型使用点审计**：扫描结果与库推荐见审计记录（ROADMAP v6.0 第 8 项）。

落地期追加发现（实现实证，已入代码与文档）：

- **gms autocommit 事务生命周期**：engine 对每条语句自动 StartTransaction（`beginTransaction`），语句收尾经 TransactionCommittingIter 提交——TransactionSession 全报错会杀死所有语句。正解 = 占位 tx + 调用序判别（显式 BEGIN 入口 GetTransaction()!=nil，gms v0.20 实证），BEGIN/SAVEPOINT 显式 1235，隐式 autocommit 零开销放行；
- **错误码映射唯一落点** = engine/sqlerr.go（无网关包装层后）：gms `CastSQLError` 对 *mysql.SQLError 透传、对 go-errors kind 自识别；唯一冲突必须保留 gms 原生错误结构（Existing 行是 INSERT IGNORE/ODKU 分流的输入）；
- **忠实类型重建**走白名单文本解析（columnTypeFromText，往返恒等测试钉死）而非 gms ParseColumnTypeString（每次全量 vitess 解析，过重）；列级 DEFAULT/ON UPDATE/COLLATE 一并显式拒绝（静默丢弃 = 与用户声明不符的行为，设计原点红线）；
- **metrics 系列同步摘除**（plan_cache_*/slow_queries_total），tuning.toml 摘除 slow_query_threshold_ms/plan_cache_capacity 死参数。

## v6.x（持续交付）

- **JSON 列 msgpack 二进制形态**：rowcodec/json.go 手卷 JSON↔msgpack 遍历器（文本→解析→msgpack 写入；读侧还原 JSON 文本，key 序归一 + 数字 float64 归一——与 MySQL 二进制 JSON 同族纪律，>2^53 整数精度不担保，docs/03 §3.4）；典型文档实测 248B→210B（~85%）；非法 JSON 写入报错（gms Convert 层）与 NULL 形态端到端钉死。
- **`_ttl` 伪列 SQL 面**（docs/07 §7.1 落地）：schema 挂尾虚拟列——写入 >0 设行 TTL / 0 显式无 TTL（覆盖表级默认）/ NULL 承 default_ttl / <0 软删除；读出 = 剩余 TTL 秒（PTTL 自省搭同一 pipeline；含 _ttl 投影绕过行级近缓存——条目不携带行剩余 TTL，默认关闭的行缓存取舍可控）。**UPDATE 不提 _ttl 保留行当前 TTL**（write_row.lua v5 `ttlms=-2` keep 分支：行 TTL/登记册照旧、回执按新成员重写且宽限=行剩余 PTTL+300s；新行无可保则登记不过期）。SELECT * 含 _ttl 列（gms 无隐藏列机制、`*` 展开为显式投影——与原稿"SELECT * 不含伪列"的诚实偏差）。实现期实证：miniredis 物理过期纯由 FastForward 驱动（真实 1ms TTL 不会自行过期——TTL 测试必须 FastForward）。
- **DROP TABLE 大表清理作业化**：超阈值（`drop_sync_max_rows` 默认 4096）→ Catalog 立即删除 + `c:dropjobs` 登记（def 快照自包含），JobRunner 按 slot 游标清理（SweepSlot 清过期残留 → 登记册分页"处理即移出"——活行 DeleteRow 全清、偏斜死行 SweepPksForced 强扫）；崩溃安全 = 作业先登记后删 Catalog + 在途防护跳过 + 换实例接管（测试钉死）。**实现期发现并修复死锁形态**：DeleteRow 对物理过期行是 no-op（write_row D 分支不撤 exp），若按"删到空为止"分页会在死行上无限空转；同页恒读首页 + 死行即批强扫，终止由构造保证。附带纪律修复：bucketmap 的 Key/RegistryKey 手拼 key 回收进 keycodec（BucketMapSlotKey/BucketMapHotKey），删除形态错误的死函数 BucketMapKey。
- **对账角色落地**（docs/12 §12.8）：`controller.Reconciler`——每 tick 每表抽样 slot 做"数据侧推导 vs 索引实际"比对（正向成员/score 校验 + 反向孤儿成员 + 唯一预约占有者活性），漂移进 `reconcile_drift_total{kind}` + 告警，**只观测不自动修复**（漂移=内核 bug 信号）；TTL 清扫暂态（回执在）不误报，异步/回填中索引跳过合法窗口；采样参数入 tuning.toml（reconcile_slots_per_tick/reconcile_rows_per_slot）。反向孤儿判定实现期修正：桶成员 pk 不在登记册取样面时直查行/回执活性（从未存在的 pk 无从判定——测试钉死）。
- **cnt 行计数器移除**：`cnt:{table}:{stag}` 只写不读（write_row/sweep_batch 维护、零消费方——COUNT(*) 任意时刻精确由 exp 登记册 Σ ZCOUNT 承接）。write_row.lua v4 / sweep_batch.lua v2 KEYS 布局收缩（exp=n-1、rcpt=n）；docs/12 §12.8 的"cnt 校准"项以移除方式闭环（校准一个没人读的计数器是纯放大）。

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
- **设计原点落地（v5.1 中段）**：`把简单留给用户，把复杂留给自己；自动优于手动；约定优于配置` 写入 docs/01 §1.0 与 README。**变量表 26→3**（只留语义开关 query_allow_fullscan_tables/replica_read/hotkey_row_cache，其余转内置常量）；**开发者调优参数统一进 internal/tuning/tuning.toml**（go-toml/v2 + embed + 红线校验，改动随发布，用户指定 TOML 分层级）；**DDL COMMENT payload 只留 default_ttl/covering/async**——max_row_bytes 固定 1MB、exp_shards/expected_rows 转自动（登记册自动细分进 ROADMAP）、dimension 改实时行数判定、prefix_copy 对字符串等值/唯一索引自动开启（LIKE 'abc%' 开箱即用）；payload 严格解析（未知字段报错）。
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

## v6.x review 修复批次（2026-08-16，全量架构 review 后）

**P0**：
- 适配器显式钉住 RESP2（go-redis v9.22 静默 RESP3 默认值：push 处理器竞态挂死实证 + RESP2-only 平台兼容面）；
- L4 生命周期落地：注册按桶粒度 `r4:{encVal}:{slot}`（按值注册曾把全 slot 重定向到单 slot 副本）、Controller Tick 驱动 1s 刷新 + 冷却 3 tick 回收、读侧死副本同 pipeline EXISTS 回退源桶、@ver 伴生 key 移除（只写不读 vestigial）。

**P1（正确性，全部有回归测试）**：
- fetchRows 三修：PTTL 回复无条件消费（死行跳读曾 ri 失步吞行）、_ttl 标记只写活行、HMGET 加 `_ver` 活性哨兵（全 NULL 回复不再误判死行/放产死行）；
- write_row.lua v6：撤字段面（UPDATE/ODKU 置 NULL 生效）+ 日志容量/回执宽限 ARGV 化（杀硬编码）；
- UPDATE 改主键撞活行 1062 判重（曾静默覆盖受害者行）；
- 标识符白名单 `[A-Za-z0-9_]{1,64}`（表/列/索引——key 布局防腐）；
- DATETIME/TIMESTAMP 小数秒精度 DDL 拒绝（存 Unix 秒，不静默截断）；
- L1 谓词指纹长度前缀编码（分隔符注入串缓存修复）；
- CREATE UNIQUE INDEX 存量查重拒建 + 回填补预约 + 冲突中止回滚（存量行曾不受唯一约束）；
- 异步索引日志转义改 url.QueryEscape 可逆形态（含空格/中文值曾永久不可见）；
- DROP 清理作业在途同名重建拒绝（旧作业曾永久卡死 + 幽灵行可见）。

**P2**：
- 时钟对齐：`kidb.SyncClock`（Redis TIME 30s 惰性重同步）接线 Guard/Exec 写读两侧；
- 预约安全：回滚/清扫释放均占有者比对（歧义窗口收窄）；
- 对账：随机页采样（首页偏倚修正）+ 唯一预约缺失方向（uniq_reservation_missing）+ bm 路由转义修正；
- telemetry 候选复核后一律摘除（注册表曾无限增长）；split 协议陈旧分片读修复 ×2；范围桶中位数改 4 分位采样；
- 指标纪律"注册即接线"：死系列 ×4 删除（sweeper_lag/async_backlog/lua_noscript/contract_violation），扇出/角色变迁/lease/桶成员/合并/热副本/配置变更接线；死 tuning 参数 ×4 删除（hot_qps/merge_*/async_log_alert_ratio）；
- 会话面：SET autocommit=0 显式拒绝（1235）；NewServer 生产路径 fail-fast（nil-fill 自愈移除）；pollSysvars/角色循环统一生命周期取消；Executor.Close 收 L1 协程；
- 退避矩阵：类型化错误分类优先（net.Error/EOF/ctx 取消）+ ±25% 抖动；
- bm.Registry / L4 ReplicaFor 1s 读缓存（每查询省 1-2 RTT）；
- 默认账号收窄 root@127.0.0.1 + 未配置告警。

**裁决回退**：JSON 列 msgpack 二进制 → 归一化文本直存（用户裁决：15% 体积收益不值得双格式编解码器与二进制不可观测；归一化保 MySQL 语义对齐）。

**docs 对齐**：02/03/05/06/08/09/10/11/12/14 全面同步（含 docs/14 的 SELECT * 含 _ttl 订正、TiDB parser 残留清除、docs/08 §8.4 L4 承诺落地对齐、docs/10 §10.3 指标表诚实化）。

### 同日后续（结构收敛）

- 客户端面全收 `kidb/kv` 包：`KvClient` → `kv.Client`（Kv 前缀移除），retry/退避装饰器、SyncClock、参考适配器（`kv/goredis`）同迁；根包收敛为 Bootstrap/错误码/Kernel 组装面；Querier 死接口删除。
- 文件名修订：`controller/autosplit.go` → `manager.go`（Manager 驱动 L4/分裂决策，不止 split）；`engine/starprobe_test.go` → `star_projection_test.go`。

### 静态检查门禁（2026-08-16）

- **golangci-lint v2.12 经 go tool 指令入库**（`tool github.com/golangci/golangci-lint/v2/cmd/golangci-lint`，与构建同 Go 版本编译——v1 路径已冻结于 1.64.x，故取 v2 模块路径）。配置 `.golangci.yml` 克制原则：默认集（errcheck/govet/ineffassign/staticcheck/unused）+ misspell/unconvert + gofmt/goimports 格式化门禁，不做风格警察；唯一豁免 = 测试清理面不查 errcheck（`defer Close`/`go Start` 的错误无行动方，测试的错误探测由断言承担）。执行入口 `make lint`（Makefile 新设 build/test/vet/lint/wire 五个快捷目标，wire 目标钉死"先删旧产物再经 module graph 运行"的实证纪律）。
- **首跑 44 项全修**：生产面 errcheck 7 处——keycodec 桶 key 尾缀解析改严格 `strconv.Atoi`（`fmt.Sscanf` 静默零值是自家生成格式的隐性宽容，畸形尾缀现在显式 ok=false，与函数其余 malformed 分支同纪律）、选举 watchdog/角色竞选 goroutine 返回值显式 `_ =`、DDL 三处流式扫描收尾 `_ =` 化；死代码 3 处——bucketmap `Shard.loaded`、topk `orderedMerger.empty`（耗尽判定由内联条件承担，字段是早期迭代残留）、sweeper_test `sprint`；staticcheck 4 处（De Morgan 展开 / 冗余类型声明 / `fmt.Sprintf("%s",…)` / Yoda 条件）；dropjob 分片循环无效首赋值 1 处。测试清理面 32 处由豁免规则承接。**修复期自伤实证**：尾缀严格化首版把 `'}'` 含进后缀段（`rest[end:]` off-by-one），ParseEqBucketKey 对全部合法 key 拒解析 → Manager 分裂复核静默全灭——`TestAutoSplitViaTelemetry` 当场抓获，补 keycodec 生成→反解往返单测（含畸形尾缀/负子桶拒绝）钉死契约。
- **连带漂移修正**（f14ee47 改名残留，本批 lint 巡检发现）：README/docs/代码注释的 `KvClient` → `kv.Client`（README 契约方法数 5→6 同步——PipelineReplica 扩面时的漏改）；docs/README 摘除"DDL 解析直接依赖 TiDB pkg/parser"声明（v6.0 已零 TiDB 依赖）。CHANGELOG/ROADMAP 历史版本条目按"历史记录不改写"纪律保留原名。
