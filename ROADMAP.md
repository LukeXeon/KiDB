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
- **keyset 分页优化**（`WHERE num > ? ORDER BY num LIMIT k` 翻译为区间起点 + 归并早停，随 top-k 一并达成）。

| 项 | 说明 |
|---|---|
| 覆盖索引读路径 | member 已携带 msgp 覆盖列（rowcodec.MemberCovers 就绪），exec 命中时跳过回表未实现 |
| L2 请求合并（同指纹并发合并） | docs/08 §8.4；落地时引入 `golang.org/x/sync`（singleflight） |
| L3 副本读（`DoReplica` 进读路径） | 适配器能力已声明，exec 未消费 |
| 并发扇出池 | 当前顺序 pipeline 批 + 批大小 bulkhead 已够用；需要结构化并发时引入 `sourcegraph/conc` / errgroup |
| 后台任务池 | 后台角色为常驻循环，无任务池负载；需要时引入 `panjf2000/ants` |
| 全扫/回填限流通道 | 落地时引入 `golang.org/x/time/rate` |
| 故障注入 | docs/12 §12.6 清单在案；引入 `Shopify/toxiproxy`（CI 环境项） |
| 行级近缓存（`hotkey_row_cache`） | 变量已注册，逻辑未实现 |
| 前缀搜索 `LIKE 'abc%'`（字典序副本 ZRANGEBYLEX 查询路径） | 副本在写，查询翻译未做 |
| HLL 基数统计接入（PFADD 写路径 + 优化器选路） | docs/04 §4.6 |
| plan cache（指纹 + 版本绑定） | docs/02 §2.6 承诺；gms 有内置语句缓存，KiDB 侧版本绑定未做 |
| EXPLAIN 自定义节点（桶数/扇出/下推展示） | docs/02 §2.8；gms EXPLAIN 可用但无 KiDB 细节 |
| 慢查询日志 | docs/10 §10.4 |

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
