# Roadmap（未实现清单）

> 本清单是唯一权威的"还没做什么"记录；每项标注性质与去向。完成即划走。
> 排序即建议优先级。

## v6.0 架构收敛（✅ 已完成，2026-08-16；破坏性变更）

> 总原则（用户裁决，2026-08-16）：**gms → 我们的转换层 → Redis**——
> 在 gms 框架内实现数据库，不是把它包装一层；网关不做任何 SQL 文本解析；
> 该删的删、该改的改，不接受过渡形态。历史快照在 `v6-wip` 分支（仅参考）。
>
> **v6.0 当新项目做**（用户同批裁决）：复用已有实现，但**不写任何兼容存量/
> 兼容老代码的过渡逻辑**（无退回旧映射、无 deprecated 层、无双格式并存）——
> 预发布阶段，字段/格式/接口变更直接演进。

| # | 项 | 内容 |
|---|---|
| 1 | 网关纯装配化 | handler 包装层全灭；gateway 只剩装配（引擎构造/账号/角色/变量/后台角色）。事务拒绝改由自定义 `engine.Session`（实现 TransactionSession 全显式报错——gms 对无该接口的会话静默 no-op BEGIN，是隐性部分提交陷阱）；ro 执法进引擎写入口/DDL 接口/SET GLOBAL 钩子（`RejectRO`）；逐语句慢日志退役（`query_duration_seconds` 指标 + 引擎层全扫告警承接） |
| 2 | 系统变量 gms 原生 | 3 个语义开关注册为 `sql.MysqlSystemVariable`（Dynamic/Global），`NotifyChanged` 经 `Session.Cfg` 持久化 cfg:global；配置面正则拦截删除 |
| 3 | 全扫闸门引擎层 | `Table.PartitionRows` 全扫前过闸：小表（<dimension_max_rows）自动放行 / 白名单放行并告警 / 否则 ERR_NO_INDEX；`/*+ FULLSCAN */` hint 通道取消（需解析才能识别，与单引擎纪律冲突） |
| 4 | 历史组件全拆 | 前置分类器、双解析器、网关快速路径（COUNT/MIN/MAX）、自定义 EXPLAIN、plan cache 判定缓存、自写指纹归一化器；**go.mod 移除 TiDB parser**（零代码依赖）。COUNT(*) 由 gms `replaceCountStar`→`StatisticsTable` 承接；MIN/MAX 经引擎聚合（端点加速写法 = ORDER BY col LIMIT 1）；EXPLAIN 走 gms 原生 |
| 5 | ddl 包并入 engine | 转换/校验即 `engine/ddlconvert.go`（快照已含） |
| 6 | DI 装配 | `github.com/google/wire` 编译期注入：全项目组件（KvClient/script/meta/exec/txguard/engine/config/controller/sweeper/indexer/nearcache/metrics/gateway）统一进 DI 图；wire_gen.go 入库，CI 校验重新生成一致 |
| 7 | 工具包收敛 | `utils/` 通用工具包：自研泛型优先队列（container/heap 封装）**替代并移除 `gopkg.in/dnaeon/go-priorityqueue.v1`**；同名/同体函数脚本扫描后归拢（回复归一 `Strings`/`StringMap`/`AnySlice`、`ParseUint64`、`SleepCtx`、`contains`→`slices.Contains`、key 布局纪律违规回收 `rowKeyOf`/`seqKeyOf`→keycodec）；测试基建 `internal/redistest` → `testutil/` 平铺 + `Probe`/`CmdCounter` 共用断言探针，`internal/tuning` → `tuning/`（独立进程非库，internal 约束无对象） |
| 8 | 泛型使用点审计 | 已扫描（2026-08-16）：手搓集合 `map[string]struct{}`（exec seen 去重 5 处、translate 去重）、`sort.SliceStable`（translate 2 处）、自写 `contains`（txguard）、container/heap 手写堆（exec/prefix.go lexHeap）、container/list LRU（plancache——v6.0 已删）。**推荐**：①stdlib 优先（Go 1.26 `slices.SortStableFunc`/`slices.Contains`/内建 min/max——零新依赖覆盖大部分）；②`utils/` 自研泛型堆（container/heap 封装，替代 dnaeon——用户指定）；③集合代数（OR 谓词 pk 求并）落地时引 `github.com/deckarep/golang-set/v2`（docs/04 §4.1 已记名）；`map[string]struct{}` 简单存在性集合保留（惯用且零开销） |
| 9 | i18n | `github.com/nicksnyder/go-i18n/v2`：用户面向消息（错误/提示）全部走消息目录（en 默认 + zh 可选）；**消息不得含技术文档引用**（docs/xx §x.y 不进用户文本，留内部注释） |
| 10 | 测试与文档对齐 | 网关薄壳相关测试重写；P5 分类器对拍退役（已落 docs/12）；docs 01/02/04/07/10/13 已按 v6.0 重写 |

完成判据（全部达成）：全测试绿 ✅ + `go.mod` 无 TiDB parser/dnaeon ✅ + gateway 无 SQL 文本处理 ✅（handler 包装层全灭，session/引擎扩展点承接执法）+ wire_gen.go 装配全组件 ✅ + i18n 消息目录落位 ✅（en 默认 + zh，`--lang` 开关；消息零 docs 引用，目录 parity 测试钉死）。

落地注记（实现期发现，docs 相应处已对齐）：

- **显式 BEGIN 判别式**：gms 对每条语句 autocommit 自动 StartTransaction——全部报错会杀死所有语句；实证调用序后采用"占位 tx + GetTransaction()!=nil ⟺ 显式 BEGIN"判别（engine/session.go）；
- **错误码映射落点**：无包装层后移入 engine/sqlerr.go（gms CastSQLError 透传 *mysql.SQLError，gms 原生 kind 错误原样放行——唯一冲突的 Existing 行结构是 IGNORE/ODKU 分流依赖）；
- **忠实类型往返**：columnTypeFromText 白名单文本解析（非 gms ParseColumnTypeString——后者每次全量 vitess 解析过重）；DEFAULT/ON UPDATE/列级 COLLATE 同步显式拒绝（静默丢弃违背设计原点）；
- **DDL COMMENT payload 内联多索引**：CREATE TABLE 内联索引经 plan.AlterIndex 逐索引调 CreateIndex（gms 路径实证），空表快速通道绕开单 _job 槽位。

## A. 正确性执法缺口——**已完成（v5.1 批次）**

- JOIN 分档执法（gateway/sqlguard.go：档 1 主键等值/档 2 维表广播放行，档 4 报错 1235）；
- 无索引谓词报错（`ERR_NO_INDEX`；`/*+ FULLSCAN */` hint 与 `query_allow_fullscan_tables` 白名单放行；索引建设中给明确提示）；
- 多行 DML 部分成功明细（非 dup 类失败携带"已提交 N 行，无整体回滚"）；**INSERT 主键判重**（1062 + UniqueKeyError 携带既有行，IGNORE/ODKU 语义经 gms 正确分流）；
- 单行体积防线 `ERR_ROW_TOO_LARGE`（docs/03 §3.4 承诺落地）；
- 副产物：修复选举器角色永不执行的**死锁**（watchdog 与角色现在并发跑；测试加反死锁断言）；
- DDL 作业：表级单 `_job` 槽位——有进行中作业则新 DDL 拒绝（避免作业覆盖）。

## B. 性能增强（正确性已由通用路径保证）

已完成（本批）：

- **top-k 归并下推**（exec/topk.go：RangeLookup 一律 16384 路 k 路归并产出全局 score 序，`go-priorityqueue` 泛型堆；顺带修复 gms `replace_sort` 删 Sort 导致的 ORDER BY 乱序隐患——探针复现后钉死；`OrderedIndex`/`CanSupport` 收紧为契约防御面；guard 补 IN/BETWEEN 列收集与 ORDER BY 范围列放行）；
- **keyset 分页优化**（`WHERE num > ? ORDER BY num LIMIT k` 翻译为区间起点 + 归并早停，随 top-k 一并达成）；
- **投影下推 + 覆盖索引读路径**（gms `ProjectedTable` 落地：回表 HMGET 子集；覆盖命中跳回表 = member 解码 + exp ZSCORE 活性校验；回填路径同步修 member 覆盖编码；DDL 覆盖列必须 NOT NULL——msgp 字符串数组 NULL 不保真；**`PrimaryKeySchema()` 恒全量**——gms coster 统计构造对窄 schema panic 的实证修复）。
- **L2 请求合并**（singleflight：EqLookup 同指纹并发合并，leader 物化 pk 列表（2^20 上限）+ 填充 L1，followers 共享后各自回表；同时补上 L1 的网关装配缺口——`nearcache_ttl/_capacity` 变量驱动 + 轮询换装，此前 L1 只在测试内接线）。
- **L3 副本读**（`DoReplica` 进读路径 + 契约新增 `PipelineReplica` 批级副本读——单命令副本读无法满足散取 RTT 纪律；参考适配器改为独立 `ReadOnly` 副本客户端（主客户端恒主节点，修正原 `ReadOnly` 主客户端会把内建只读命令全分流的误配）；开关 = `replica_read` 变量 × 能力位轮询热更）。
- **行级近缓存**（`hotkey_row_cache` 变量消费：pk→行投影，命中零 RTT；条目 TTL = min(默认 TTL, 行 PTTL)——过期行绝不返回；更新/删除陈旧窗口 ≤3s 为文档化取舍（默认关闭的根因），docs/08 §8.4 语义已如实改写）。
- **前缀搜索**（`LIKE 'abc%'` → 字典序副本 ZRANGEBYLEX k 路归并：引擎接入走 gms `IndexSearchableTable.LookupForExpressions`——FilteredTable 在 v0.20 只在 bindvar 规则被调用，是死路；`CanSupport` 对 prefix_copy 索引额外放行前缀区间形态 [p, p+\xff)；回表 HasPrefix 重判；回填补齐字典序副本产出（此前 `_job` 回填只写主桶，在线建的副本是哑的）；guard 同步放行常量前缀 LIKE）。
- **慢查询日志**（`slow_query_threshold_ms` 变量（默认 500ms）；网关统一计时：指纹 NormalizeDigest/路由/行数/耗时；全扫放行强制告警 + slow_queries_total/fullscan_fallback_total；配套补上 cmd 的指标装配缺口——metrics.New(nil) + 可选 -metrics-addr /metrics 端点，此前 SetMetrics 在生产路径未接线）。
- **EXPLAIN 自定义输出**（网关接管 `EXPLAIN SELECT`：两列计划展示——路径/索引/扇出估算/守卫判定；计划推断非执行回放；必须在快速路径之前接管，否则 EXPLAIN COUNT(*) 会真执行）。
- **plan cache 判定缓存**（指纹 NormalizeDigest + schema 版本绑定，LRU `plan_cache_capacity` 热更；缓存的是网关判定产物（fastpath 形状+守卫放行）而非计划结构；全扫依赖判定保守不缓存（随配置漂移）；顺带合并 fastpath/guard 双份解析为单 parse 联合评估；**补齐预处理语句执法缺口**——ComPrepare 此前绕过事务/ro/守卫直达引擎）。
- **全扫/回填限流通道**（`golang.org/x/time/rate`：DDL 回填按 `ddl_backfill_rate_limit` 行/s/实例限速；全扫并发信号量 `query_fullscan_rate_limit`（超限排队，ctx 贯穿）——两变量轮询热更）。
- **HLL 基数统计**（按值确定性采样 PFADD（1/64，频率无关——按 pk 采样会把低基数列高估 ~64 倍，推导后否决）；PFCOUNT×64 读侧 + EXPLAIN cardinality(approx) 行；AND 自动选路接管留作后续项——gms coster 当前承载，实测偏差 -10.4%@5000 distinct）。

| 项 | 说明 |
|---|---|
| AND 自动选路接管（LookupForExpressions 合取选低基数索引，消费 HLL 基数） | 数据面已就绪（B11）；当前由 gms coster 均匀统计承载，出现选路误判案例再启动 |
| 并发扇出池 | 当前顺序 pipeline 批 + 批大小 bulkhead 已够用；需要结构化并发时引入 `sourcegraph/conc` / errgroup（条件项） |
| 后台任务池 | 后台角色为常驻循环，无任务池负载；需要时引入 `panjf2000/ants`（条件项） |
| 故障注入 | docs/12 §12.6 清单在案；引入 `Shopify/toxiproxy`（CI 环境项） |

## C. 运维与验证基建

| 项 | 说明 |
|---|---|
| 多节点集群契约场景（MOVED/ASK 迁移窗口） | contract/ 现为单节点全 slot；多节点编排需 docker 环境（CI 项） |
| GUI 客户端握手门禁（DBeaver/Navicat/DataGrip 实连）+ pymysql | 用户明确暂缓；go-sql-driver 已在冒烟覆盖 |
| 对账任务（抽样对账/cnt 校准/预约 key 残留巡检） | docs/12 §12.8；长期运行项 |
| 性能基准门禁（k6/自研压测，1 亿行数据集） | docs/12 §12.7 指标在案；内核侧基线基准已落地（exec/bench_test.go + shape_test.go 命令形状不变式），缺真实集群门禁 |
| exp 登记册自动细分（体积超阈自动重散列，docs/07 §7.2 容量账） | 分片键机制保留在 keycodec；当前恒 1 分片，10 亿行+ 表触碰 8MB 红线（文档已声明） |
| DROP TABLE 大表后台清理作业 | 当前同步清理，小表成立 |
| `_ttl` 伪列 SQL 面（行级 TTL 显式读写） | docs/07 §7.1；当前表级 default_ttl 已生效 |
| JSON 列 msgp 压缩 | 文本形态正确无虞；压缩为体积优化（`_fmtv` 演进路径在案） |

## D. 有意不做（红线，docs/14）

跨 slot 多行事务、大表任意 JOIN、强一致读、TRUNCATE、GRANT/REVOKE、MVCC 全家桶、fork TiDB parser、自研 SQL 解析器——见 docs/14 §14.1，立项才动。
