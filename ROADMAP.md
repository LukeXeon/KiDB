# Roadmap（未实现清单）

> 当前状态：SQL 服务器端到端可用（读写/DDL/自治/清扫/配置/指标全链路有测试覆盖）。
> 本清单是唯一权威的"还没做什么"记录；每项标注性质与去向。完成即划走。
> 排序即建议优先级。

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
| 性能基准门禁（k6/自研压测，1 亿行数据集） | docs/12 §12.7 指标在案 |
| DROP TABLE 大表后台清理作业 | 当前同步清理，小表成立 |
| `_ttl` 伪列 SQL 面（行级 TTL 显式读写） | docs/07 §7.1；当前表级 default_ttl 已生效 |
| JSON 列 msgp 压缩 | 文本形态正确无虞；压缩为体积优化（`_fmtv` 演进路径在案） |

## D. 有意不做（红线，docs/14）

跨 slot 多行事务、大表任意 JOIN、强一致读、TRUNCATE、GRANT/REVOKE、MVCC 全家桶、fork TiDB parser、自研 SQL 解析器——见 docs/14 §14.1，立项才动。
