# 02 · SQL 服务器（核心）

> **v6.0 架构原则（设计原点 docs/01 §1.0 的直接推论）**：
> **gms 直接承接 SQL，然后才到我们的代码——我们在 gms 框架内实现数据库，不是把它包装一层。**
> 网关不做任何 SQL 文本解析/分流/预解析优化（那是重复 gms 的劳动）；
> 一切语义落在 gms 扩展点（Database/Table/Index/Session/系统变量）与内核执行器上。

## 2.1 协议层（go-mysql-server wire server）

协议层直接采用 go-mysql-server 的 `server` 包（MySQL wire protocol 实现），**无 handler 包装层**：

| 能力 | 来源 | 说明 |
|---|---|---|
| 握手/认证 | gms `server` + MySQLDb | `mysql_native_password`；账号在引导配置声明（只读/读写两级，见 §2.8） |
| COM_QUERY / COM_STMT_PREPARE / COM_STMT_EXECUTE / COM_STMT_CLOSE / COM_STMT_SEND_LONG_DATA / COM_PING / COM_QUIT / COM_INIT_DB | gms | COM_INIT_DB 映射表命名空间前缀（§2.4） |
| 结果集流式写出 | gms `sql.RowIter` | 与内核 RowIter 全链路流式对接（[04](04-查询路径.md) §4.3），永不物化全量 |
| TLS | gms server TLS 配置 | 可选，按部署需要开启；内网部署默认关闭 |
| 连接数/超时 | gms server 配置 | `max_connections`、读写超时（引导配置，[10](10-配置与可观测.md) §10.2） |
| 慢查询观测 | gms `ServerEventListener` + 指标 | `query_duration_seconds{plan}` 直方图 + 引擎层全扫告警；无逐语句文本日志（v6.0 简化裁决） |

**连接生命周期**：accept → 握手认证 → 会话对象创建（KiDB Session：角色 + 配置存储句柄）→ 命令循环 → 关闭。会话是**无事务态**的：缓存定位，不支持 BEGIN/COMMIT/ROLLBACK——**事务语义由自定义 Session 类型显式拒绝**（见 §2.4）。

## 2.2 单引擎纪律（v6.0）

**一切语句进 gms 引擎；没有前置分类器、没有第二解析器、没有网关侧 SQL 分析。**

```
ComQuery(sql)
  → gms wire handler（其 parser 解析）
  → gms analyzer（投影下推/索引选择/top-k 删 Sort 等分析器规则）
  → KiDB 扩展点执行：
      Database（TableCreator/TableDropper）——DDL 建表/删表
      Table（IndexAlterableTable/IndexedTable/ProjectedTable/StatisticsTable…）——索引增删/扫描
      exec 执行器（散取/归并/回表校验）
      txguard（写入 Lua 编排）
```

纪律与边界：

- **网关只做装配**（引擎构造、账号注册、后台角色启动、能力探测、限流/告警挂点）；
- **DDL 全量经 gms**：planbuilder 产出计划节点 → 我们的 `Database.CreateTable`（`sql.TableCreator`，COMMENT 串直达）/ `Table.CreateIndex`（`sql.IndexAlterableTable`，`IndexDef.Comment` 直达）——校验与 Catalog 作业化语义不变（[06](06-元数据与Schema演进.md) §6.3），类型语义以 gms 为准（`types.Is*` 谓词映射到存储列类型）；
- **一个定制分析器规则变更**：移除 `resolveAlterColumn`（OnceBeforeDefault）——它按"TEXT/BLOB 索引须前缀长度"的 MySQL 习惯校验，与桶模型冲突（字符串列无前缀长度概念）；
- **历史拆除**：v5.x 的前置分类器（classify）、双解析器分工、网关快速路径（COUNT(*)/MIN/MAX 形状识别）、网关自定义 EXPLAIN、plan cache 判定缓存、网关 sqlguard 文本分析——全部删除。COUNT(*) 由 gms `replaceCountStar` 规则原生承接（命中 `sql.StatisticsTable` 精确 RowCount，即 exp 登记册 Σ ZCOUNT）；MIN/MAX 经引擎聚合（结果精确），端点加速的等价写法是 `ORDER BY col LIMIT 1`（top-k 有序流早停）。

## 2.3 DDL 路径（gms 托管）

| 语句 | gms 路径 | KiDB 承接 |
|---|---|---|
| `CREATE TABLE` | `plan.CreateTable` | `Database.CreateTable(ctx, name, PrimaryKeySchema, collation, comment)`——COMMENT 串直达 |
| `CREATE [UNIQUE] INDEX` / `ALTER TABLE ADD INDEX` | `plan.AlterIndex`（btree/hash 默认路径） | `Table.CreateIndex(ctx, sql.IndexDef)`——`IndexDef.Comment`/`Constraint` 直达 |
| `DROP TABLE` | `plan.DropTable` | `Database.DropTable` |
| `DROP INDEX` / `ALTER DROP INDEX` | `plan.AlterIndex` | `Table.DropIndex` |
| `TRUNCATE TABLE` | gms 计划节点 | 不支持（无界操作，[01](01-定位架构与TiDB对齐.md) §1.2） |
| 其余 ALTER（加减列/改类型等） | — | 不实现对应接口 → gms 明确报错 |

列类型白名单（gms 类型语义）：整数族（`types.IsInteger`）、浮点（`IsFloat`）、字符串（`IsText`）、二进制（`IsBinaryType`）、`DATETIME/TIMESTAMP`（存 Unix 秒）、`JSON`。DECIMAL/DATE/TIME/枚举等明确报错（score 精度与编码纪律）。**注意**：`types.IsBinaryType` 把 JSON 算二进制——映射判定必须先 JSON 后二进制（实现教训）。

KiDB 扩展的 COMMENT 载体（不 fork parser 的关键，语义不变）：`COMMENT 'kidb:{json}'`，表级仅 `default_ttl`，索引级仅 `covering`/`async`（严格解析，未知字段报错——docs/01 §1.0：其余选项全部自动或内置）。DDL 作业化（在线建索引、`Building` 不可见、`_job` 断点续作）语义见 [06](06-元数据与Schema演进.md) §6.3；空表建索引同步完成（无回填对象，绕开单 `_job` 槽位——CREATE TABLE 内联多索引场景），非空表走 `_job` 后台回填。

## 2.4 会话状态（engine.Session）

每会话维护（自定义 `engine.Session`，嵌 gms BaseSession）：

| 状态 | 语义 |
|---|---|
| 角色 | `rw`/`ro`，连接期从账号表注入；ro 执法在写路径各入口（编辑器 Insert/Update/Delete、DDL 接口、SET GLOBAL 钩子）统一 `RejectRO` |
| 配置存储句柄 | `Cfg *config.Store`——SET GLOBAL 的持久化桥（sysvar `NotifyChanged` 经会话找到本实例配置存储，见 §2.6） |
| 事务表面 | 实现 `sql.TransactionSession` 全部方法并**显式报错**——gms 对不实现该接口的会话**静默 no-op** BEGIN（客户端会误以为事务生效），必须实现后报错 |
| `LAST_INSERT_ID()` | AUTO_INCREMENT 写入后由 TxGuard 经会话状态回填（[05](05-写入路径.md) §5.4） |
| 预处理语句注册表 | gms 原生承载（PREPARE 解析缓存，EXECUTE 复用）；DDL 经预处理协议同样工作（全量走引擎后无分叉） |

## 2.5 Plan cache

v6.0 起网关侧不存在计划缓存。v5.x 的"判定缓存"（指纹 + schema 版本绑定）存在的理由是摊薄网关侧二次解析——网关不再解析 SQL 后该缓存失去对象。gms 内建的语句缓存与预处理注册表照常工作（引擎内部面，无需我们重复建设）。

## 2.6 EXPLAIN

走 gms 原生 EXPLAIN（计划树展示：IndexAccess/索引名/范围等）；KiDB 侧细节经 `Index.String()` 等 gms 展示面透出。v5.x 的网关自定义 EXPLAIN 已拆除。

## 2.7 系统变量（gms 原生 sysvar 机制）

KiDB 的三个语义开关注册为 gms 原生系统变量（`sql.MysqlSystemVariable`，Dynamic，Global 作用域）：

| 变量 | 默认 | 说明 |
|---|---|---|
| `query_allow_fullscan_tables` | `''` | 全表遍历表白名单（逃生门，[07](07-TTL与过期清扫.md) §7.4） |
| `replica_read` | `false` | L3 副本读开关（适配器能力位缺失时自动无效，[09](09-后端契约与适配器.md) §9.4） |
| `hotkey_row_cache` | `false` | 行级读热点近缓存（陈旧窗口语义，[08](08-自治治理与热Key.md) §8.4） |

- `SET GLOBAL x = v` 经 gms sysvar 机制校验 → `NotifyChanged` 钩子经会话的 `Cfg` 句柄持久化到 `cfg:global`（CAS + `_ver` 递增 + `_audit` 审计，[10](10-配置与可观测.md) §10.2）；多实例经版本校验轮询秒级收敛；
- `SHOW [GLOBAL] VARIABLES` / `SELECT @@global.x` 由 gms 原生服务；`SET SESSION` 会话级覆盖不落盘（gms 语义）；
- ro 账号 SET GLOBAL 在 `NotifyChanged` 钩子内拒绝（`RejectRO`）；
- 命名纪律：小写下划线；**变量只承载语义开关**（调优参数在 `tuning/tuning.toml`，[10](10-配置与可观测.md) §10.2）。

## 2.8 错误码与权限

错误码映射不变：1062 唯一冲突（预约 key 判定，[05](05-写入路径.md) §5.3）/ 1235 超定位（含事务）/ 1290 只读 / 1105 集群类与耗尽类 / `ERR_NO_INDEX`（引擎层全扫闸门拒绝）/ `ERR_ROW_TOO_LARGE` / `ERR_INDEX_LOG_FULL` / `ERR_STALE_METADATA`（内部重试耗尽 1197）。

权限边界：账号在引导配置声明（`user/host/password/role`，role ∈ {`rw`,`ro`}）。无 GRANT/REVOKE——部署边界问题（网络隔离 + 账号分级）。**ro 执法点全部在引擎扩展点内**：写入编辑器、DDL 接口、SET GLOBAL 钩子。

## 2.9 握手兼容矩阵（采纳度生死线）

不变：GUI 工具与驱动的握手探测语句必须礼貌应答——`SELECT @@version` 等由 gms 原生服务；`SHOW TABLES`/`SHOW CREATE TABLE`/`SHOW INDEX` 从 Catalog 生成；`INFORMATION_SCHEMA` 内存视图；`SET NAMES`/`SET autocommit`/`USE db` 会话状态接受；不支持的会话变量返回默认值 + debug 日志，不报错。兼容矩阵与门禁见 [12](12-测试方案.md) §12.5。
