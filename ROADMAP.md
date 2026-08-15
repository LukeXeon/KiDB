# Roadmap（未实现清单）

> 当前状态：SQL 服务器端到端可用（读写/DDL/自治/清扫/配置/指标全链路有测试覆盖）。
> 本清单是唯一权威的"还没做什么"记录；每项标注性质与去向。完成即划走。
> 排序即建议优先级。

## A. 正确性执法缺口（最高优先）

| 项 | 说明 | 去向 |
|---|---|---|
| **JOIN 分档执法** | docs/04 §4.4 承诺档 4（大表任意 JOIN）报错 `ERR_UNSUPPORTED_JOIN`；当前 gms 引擎会执行（内存 hash join 全量拉表）——需自定义 analyzer 规则或网关节点拦截有界性 | 下一批 |
| **无索引谓词报错**（`ERR_NO_INDEX` + FULLSCAN hint/白名单通道） | docs/04 §4.1：无索引谓词默认报错；当前全扫直接执行。拦截点在引擎/网关 | 下一批 |
| **多行 DML 的部分成功明细** | docs/05 §5.5：跨 slot 多行写逐行执行、失败返回部分成功明细；当前依赖引擎逐行调用，失败语义未按文档化 | 下一批 |

## B. 性能增强（正确性已由通用路径保证）

| 项 | 说明 |
|---|---|
| top-k 归并下推（ORDER BY + LIMIT 走桶端点） | 当前引擎层 sort；docs/04 §4.1 的 k 路归并 |
| 覆盖索引读路径 | member 已携带 msgp 覆盖列（rowcodec.MemberCovers 就绪），exec 命中时跳过回表未实现 |
| L2 请求合并（singleflight 同指纹并发合并） | docs/08 §8.4；未接线 |
| L3 副本读（`DoReplica` 进读路径） | 适配器能力已声明，exec 未消费 |
| 行级近缓存（`hotkey_row_cache`） | 变量已注册，逻辑未实现 |
| 前缀搜索 `LIKE 'abc%'`（字典序副本 ZRANGEBYLEX 查询路径） | 副本在写，查询翻译未做 |
| HLL 基数统计接入（PFADD 写路径 + 优化器选路） | docs/04 §4.6 |
| keyset 分页优化 | 引擎层 offset 承载中 |
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
