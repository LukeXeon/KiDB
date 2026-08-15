# 02 · SQL 服务器（核心）

> 本章是 v5.0 的中心章：KiDB 以 **MySQL 协议网关** 交付，SQL 服务器是唯产品形态。
> 解析策略：**DDL 走 TiDB `pkg/parser`（go.mod 直接依赖，零修改），DML 走 go-mysql-server 引擎**，
> 前置分类器负责把语句路由到正确的解析路径。复用论证见 [13](13-TiDB复用清单.md)。

## 2.1 协议层（go-mysql-server wire server）

协议层直接采用 go-mysql-server 的 `server` 包（MySQL wire protocol 实现），不自研协议编解码：

| 能力 | 来源 | 说明 |
|---|---|---|
| 握手/认证 | gms `server` + 自定义 AuthBackend | `mysql_native_password`；账号在引导配置声明（只读/读写两级，见 §2.9） |
| COM_QUERY / COM_STMT_PREPARE / COM_STMT_EXECUTE / COM_STMT_CLOSE / COM_STMT_SEND_LONG_DATA / COM_PING / COM_QUIT / COM_INIT_DB | gms | COM_INIT_DB 映射表命名空间前缀（§2.5） |
| 结果集流式写出 | gms `sql.RowIter` | 与内核 RowIter 全链路流式对接（[04](04-查询路径.md) §4.3），永不物化全量 |
| TLS | gms server TLS 配置 | 可选，按部署需要开启；内网部署默认关闭 |
| 连接数/超时 | gms server 配置 | `max_connections`、读写超时（引导配置，[10](10-配置与可观测.md) §10.4） |

**连接生命周期**：accept → 握手认证 → 会话对象创建（会话 id、用户、默认命名空间）→ 命令循环（每条语句经前置分类器）→ 关闭（释放预处理语句注册表、会话变量 overlay）。会话是**无事务态**的：缓存定位，不支持 BEGIN/COMMIT/ROLLBACK（收到一律报错 1235，`ERR_UNSUPPORTED`），所有语句等价 autocommit。

## 2.2 前置分类器（gateway/classify）

每条语句在进入引擎前，先经前置分类器决定解析路径。**分类器是唯一允许"看裸 SQL 文本"的组件**，两个引擎互不感知对方的存在。

```
StripComments(sql) → 首一/二个关键字（大小写不敏感）→ 路由：

CREATE TABLE / CREATE [UNIQUE] INDEX / DROP TABLE / DROP INDEX / ALTER TABLE
    → KiDB DDL 路径（TiDB parser 解析，§2.4）
其余（SELECT/INSERT/UPDATE/DELETE/REPLACE/SHOW/SET/USE/EXPLAIN/DESCRIBE/…）
    → go-mysql-server 引擎（DML 路径，§2.5）
```

实现要点：

- `StripComments` 处理 `/* */`、`-- `、`#` 三类注释，且对字符串字面量敏感（`'…'`/`"…"`/反引号内不剥）；~100 行，纯函数；
- DDL 判定集是**封闭白名单**（上表五种形态），判不准的一律走 DML 路径——宁可漏给引擎报错，不可错抢；
- `CREATE UNIQUE INDEX` 两个关键字都要识别；`CREATE FULLTEXT/SPATIAL INDEX` 识别后走 DDL 路径再明确报错（超出定位）；
- **正确性对拍**：分类器与 TiDB parser 的语句类型判定做 PBT 对拍（随机 SQL 语料 + 变异注释/空白/字符串，断言分类结果 == AST 顶层节点类型），见 [12](12-测试方案.md) §12.3 P5。

**为什么不做单解析器**：两个引擎的 AST 体系不可互注（无法把 TiDB AST 喂给 go-mysql-server）。若用 TiDB parser 统一解析再分发，DML 仍要被 gms 二次解析（gms 只认自己的 AST），等于每条 DML 双份解析开销。前置分类器把"分类"做到 O(首关键字)，DML 只被 gms 解析一次，DDL（低频）独享 TiDB parser 的 MySQL 语法保真度。

## 2.3 双解析器分工纪律

| | DDL 路径 | DML 路径 |
|---|---|---|
| 解析器 | `github.com/pingcap/tidb/pkg/parser`（独立 module，go.mod 锁版本） | go-mysql-server 内建 parser |
| 原因 | 需要 MySQL 保真的建表/建索引语法 + KiDB 扩展载体 | 需要分析器/执行器/wire/sysvar 全家桶，gms 一站提供 |
| 频率 | 低频（管理面） | 高频（数据面） |
| 一致性风险 | 两解析器语法覆盖不完全一致（如某边缘语法一边能过一边报错） | — |
| 缓解 | DDL 语法面刻意取两解析器交集（标准 MySQL DDL 子集，§2.4）；差异用例进测试（[12](12-测试方案.md) §12.5） | — |

**纪律**：DDL 路径产出 Catalog 定义后直接落库，绝不把 DDL 文本透传给 gms；DML 路径绝不经过 TiDB parser。两个路径的语法面不追求互相兼容对方全集，只保证各自文档化子集。
**v5.1 注记**：TiDB parser 在 DML 侧有一处**识别性**使用——网关快速路径（COUNT(*)/MIN/MAX 白名单形状识别，[04](04-查询路径.md) §4.1/§4.5）；它只回答"形状是否命中白名单"，执行语义仍归 gms/内核执行器，判不准一律回退引擎路径。

## 2.4 DDL 路径（ddl 包）

### 支持的 DDL 子集

| 语句 | 支持度 | 说明 |
|---|---|---|
| `CREATE TABLE` | ✅ 受限子集 | 列类型白名单（下表）；必须显式主键（单列）；KiDB 表选项经 COMMENT 载体 |
| `CREATE INDEX` / `CREATE UNIQUE INDEX` | ✅ | 等值/范围/前缀副本由列类型与选项推导；在线构建（[06](06-元数据与Schema演进.md) §6.3） |
| `DROP TABLE` / `DROP INDEX` | ✅ | DROP TABLE 走"标记下线 + 后台清扫"（借 exp 登记册遍历逐批清理，不阻塞） |
| `ALTER TABLE` | ⚠️ 极小集 | 仅 `ADD INDEX`/`DROP INDEX`（映射独立作业）；加减列/改类型 v5.0 不支持（报错 1235，引导重建表） |
| `TRUNCATE TABLE` | ❌ | 无界操作，报错（[01](01-定位架构与TiDB对齐.md) §1.2） |

列类型白名单：`BIGINT/INT/TINYINT/BOOLEAN`、`DOUBLE/FLOAT`、`VARCHAR(n)/CHAR(n)`、`VARBINARY/BLOB`、`DATETIME/TIMESTAMP`（存 Unix 秒）、`JSON`（嵌套 blob，msgp 编码）。范围索引列必须是数值或时间戳且 int64 不越 2^53（[03](03-数据模型与编码.md) §3.4）。

### KiDB 扩展的 COMMENT 载体（不 fork parser 的关键）

TiDB parser 原生解析表级/索引级 `COMMENT 'string'` 选项（`ast.TableOptionComment`、`ast.IndexOption.Comment`），KiDB 的全部自定义 DDL 参数以 JSON payload 承载于 COMMENT，**parser 零修改**：

```sql
CREATE TABLE sessions (
  uid BIGINT NOT NULL,
  token VARCHAR(64) NOT NULL,
  profile JSON,
  PRIMARY KEY (uid)
) COMMENT 'kidb:{"default_ttl":86400,"max_row_bytes":1048576,"expected_rows":"1e8","exp_shards":16}';

CREATE INDEX idx_token ON sessions (token)
  COMMENT 'kidb:{"prefix_copy":true}';            -- 前缀搜索字典序副本

CREATE INDEX idx_profile ON sessions (uid)
  COMMENT 'kidb:{"covering":["token"],"async":false}';  -- 覆盖索引；异步索引不允许 covering
```

payload 字段全集与默认值在 [06](06-元数据与Schema演进.md) §6.1；DDL 校验规则（索引数 ≤16、列数 ≤256、`_` 前缀列拒绝、覆盖索引必须同步、async+unique 互斥等）在执行器内 fail-fast。COMMENT 里非 `kidb:` 前缀的内容视为普通注释，两不干扰。

### DDL 作业化

DDL 不是一条命令的结束，而是一个作业的开始：校验 → Catalog CAS 落库（`_ver` 单调递增）→ 后台在线建索引（回源回填 + 增量追平）→ 对外可见。完整状态机与断点续作见 [06](06-元数据与Schema演进.md) §6.3。DDL 期间旧 plan 全部失效（§2.6 版本绑定保证）。

## 2.5 会话状态（gateway/session）

每会话维护：

| 状态 | 语义 |
|---|---|
| 当前命名空间 | `USE db` 接受并记录。**v1 实现为扁平命名空间**（表名全局唯一，不加前缀）；多租户前缀隔离列入后续 |
| 会话变量 overlay | `SET SESSION x=v` 只写会话层，不落 `cfg:global`（[10](10-配置与可观测.md) §10.2）；不支持的会话变量返回默认值 + debug 日志，**不报错**（握手兼容生死线） |
| `LAST_INSERT_ID()` | AUTO_INCREMENT 写入后由 TxGuard 经会话状态回填（[05](05-写入路径.md) §5.4） |
| `FOUND_ROWS()` / `ROW_COUNT()` | 按 MySQL 语义由执行器回填 |
| 预处理语句注册表 | COM_STMT_PREPARE 解析并缓存（stmt id → 指纹 + 参数位），COM_STMT_EXECUTE 复用 plan cache；连接关闭即销毁 |
| 事务状态 | 恒为"无事务"；`BEGIN/COMMIT/ROLLBACK/START TRANSACTION` 一律报错 1235 |

## 2.6 Plan cache（版本绑定，对齐 TiDB plan cache 纪律）

- **指纹**：`parser.NormalizeDigest(sql)`（TiDB parser 独立模块自带）产出 (normalized, digest) 作为缓存键主体；键还包含：当前命名空间、`schema_ver`、`bm_ver` 快照代际、影响计划的会话变量（如 `query_allow_fullscan_tables` 会话覆盖）；
- **条目绑定版本**：缓存条目记录生成时的 `{catalog_ver, bm_version}`；命中前比对当前版本，不一致则重建——**DDL 上线、索引可见、桶分裂布局变化后旧计划绝不复用**（对齐 TiDB plan cache 的 schema-version 校验，`pkg/planner/core/plan_cache*.go`）；
- 失效因此是**惰性且精确的**：不需要主动广播失效，版本比对自然完成；
- 容器：LRU 1024 条（`plan_cache_capacity` 变量）；Prepared Statements 参数化后指纹稳定，命中率依赖预处理协议的正确实现（§2.5）；
- 防注入标准姿势：引导业务一律 Prepared Statements。

## 2.7 系统变量（配置即数据的 SQL 面）

go-mysql-server 原生 system variable 机制注册 KiDB 全部变量；`SET GLOBAL` 的持久化后端挂载到 `cfg:global`（`config_set.lua` CAS），`SET SESSION` 走会话 overlay。变量全集、默认值、校验规则与传播机制见 [10](10-配置与可观测.md) §10.2。命名纪律：小写下划线，不用点号。

## 2.8 EXPLAIN

自定义 EXPLAIN 节点展示 KiDB 执行细节：命中索引、桶集合与数量、扇出度、是否谓词下推、是否走副本/近缓存、回表批数估算。慢查询日志携带同一计划摘要（[10](10-配置与可观测.md) §10.4）。

## 2.9 错误码与权限

### 错误码映射

| 内核错误 | MySQL 错误码 | 说明 |
|---|---|---|
| `ERR_DUPLICATE_KEY` | 1062 | 唯一冲突（预约 key 判定，[05](05-写入路径.md) §5.3） |
| `ERR_UNSUPPORTED` / `ERR_UNSUPPORTED_JOIN` | 1235 | 档 4 JOIN、事务语句、TRUNCATE、GRANT、超范围 DDL/类型 |
| `ERR_NO_INDEX` | 自定义 | 无索引谓词且未开全扫 |
| `ERR_ROW_TOO_LARGE` | 自定义 | 超 `max_row_bytes`（[03](03-数据模型与编码.md) §3.4） |
| `ERR_INDEX_LOG_FULL` | 自定义 | 异步索引日志背压（[05](05-写入路径.md) §5.2） |
| `ERR_STALE_METADATA` | 内部重试，耗尽后 1197 | Catalog/BucketMap 版本冲突（[06](06-元数据与Schema演进.md)） |
| `ERR_REDIRECT_EXHAUSTED` | 1105 | 集群迁移窗口重试耗尽（[09](09-后端契约与适配器.md) R5） |
| `ERR_CLUSTER_UNAVAILABLE` | 1105 | CLUSTERDOWN/LOADING 退避耗尽（[09](09-后端契约与适配器.md) §9.6） |
| `ERR_READ_ONLY` | 1290 | 只读账号执行写语句 |
| `ERR_CAPABILITY` | 启动期拒绝 | EVAL 缺失等能力探测失败（[09](09-后端契约与适配器.md) §9.4） |

### 权限（v5.0 边界）

账号在引导配置声明：`user/host/password/role`，role ∈ {`rw`, `ro`}。`ro` 账号执行 DML 写/DDL/SET GLOBAL 报 `ERR_READ_ONLY`。无 GRANT/REVOKE、无库表级 ACL——缓存查询层定位，权限是部署边界问题（网络隔离 + 账号分级），不是 SQL 层功能（[14](14-红线局限与检查单.md)）。所有 DDL 与 SET GLOBAL 写入 `cfg:global._audit` 审计字段。

## 2.10 握手兼容矩阵（采纳度生死线）

GUI 工具与驱动连接后立即发出握手探测语句，必须礼貌应答，否则客户端直接断开：

- `SELECT @@version` / `@@version_comment` 等：返回构造值（version 伪装 MySQL 8.0.x，version_comment 标识 KiDB）；
- `SHOW TABLES` / `SHOW CREATE TABLE` / `SHOW INDEX`：从 Catalog 生成；
- `INFORMATION_SCHEMA.{TABLES,COLUMNS,STATISTICS}`：内存视图从 Catalog 填充；
- `SET NAMES` / `SET autocommit` / `USE db`：会话状态接受并记录，无副作用语句一律 OK；
- 不支持的会话变量：返回默认值 + debug 日志，不报错。

兼容矩阵（发版门禁，[12](12-测试方案.md) §12.5）：DBeaver、Navicat、DataGrip、go-sql-driver/mysql、pymysql、MySQL CLI。每客户端一条真实连接脚本，覆盖：握手 → `USE` → `SHOW TABLES` → 一条 SELECT → 一条预处理语句。
