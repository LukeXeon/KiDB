# KiDB

**把 Redis Cluster 变成一张会讲 SQL 的缓存表。** KiDB 是 Redis 集群上的分布式 SQL 缓存查询层——MySQL 协议网关交付，业务方只需要会 SQL：不用再设计 key 命名规范、手写二级索引、处理 cache-aside 的回填/失效时序。

```sql
CREATE TABLE sessions (
  uid BIGINT NOT NULL, token VARCHAR(64), age INT,
  PRIMARY KEY (uid),
  INDEX idx_age (age)
) COMMENT 'kidb:{"default_ttl":86400}';

INSERT INTO sessions VALUES (42, 'tok_abc', 30);
SELECT * FROM sessions WHERE age >= 18 AND age <= 60;   -- 走范围索引桶
SELECT COUNT(*) FROM sessions;                          -- exp 登记册汇总，任意时刻精确
```

任意 MySQL 客户端/驱动/GUI 直连（DBeaver、Navicat、go-sql-driver、pymysql……）。

## 特性

- **MySQL 协议/语法子集**：等值/范围/AND/OR/ORDER BY/LIMIT/COUNT/MIN/MAX/前缀 LIKE、INSERT/UPDATE/DELETE/UPSERT、AUTO_INCREMENT、Prepared Statements；
- **二级索引**：同步（默认）与异步（写热点字段）两种模式，在线构建（后台作业 + 断点续作）；
- **行级 TTL**：过期即查不到，分布式 Sweeper 清扫索引残留；
- **大 key/热 key 自治**：索引桶在线分裂/合并（无停写、无读空洞），热桶副本多 slot 摊开读，进程内近缓存；
- **零 SCAN 依赖**：全链路 ZSet 分页（适配禁用 keyspace SCAN 的生产平台）；
- **正确性纪律**：结果必须精确（回表校验兜底一切异步路径），统计可以近似；
- **后端可替换**：内核只面向 `KvClient` 窄接口（5 个方法），go-redis/v9 参考适配器内置；任何满足[契约](docs/09-后端契约与适配器.md)的客户端均可接入。

## 快速开始

```bash
go build ./cmd/kidb-server
./kidb-server --listen :3306 --redis 127.0.0.1:6379 \
  --accounts 'root:%::rw,reader:%::ro'
mysql -h 127.0.0.1 -P 3306 -u root   # 连上即用
```

## 架构一页纸

无状态 SQL 计算层 + 共享 KV：行存 Hash、索引存 slot 内聚的 ZSet 桶、过期存登记册，hash tag 保证同行数据同 slot；写入原子性由单 slot Lua 提供。架构上与 TiDB 同构（无状态 SQL 层 + 分布式 KV），差异全部来自 KV 层能力差——逐机制推导见 [docs/01](docs/01-定位架构与TiDB对齐.md) 与 [docs/13](docs/13-TiDB复用清单.md)（含"为什么不直接搬 TiDB"的尸检）。

```
MySQL 客户端 → kidb-server（协议层/分类器/双解析器）→ 内核（planner/exec/txguard）
             → KvClient 适配器 → Redis Cluster
```

## 文档

完整设计文档在 [docs/](docs/README.md)：范围与架构、SQL 服务器、数据模型、查询/写入路径、元数据演进、TTL 清扫、自治治理、后端契约、配置与可观测、部署运维、测试方案、TiDB 复用清单、红线与上线检查单。变更历史见 [CHANGELOG.md](CHANGELOG.md)。

## 测试

```bash
go test ./...          # 单测 + PBT（miniredis，真实 Lua 执行）
go test ./contract/    # 适配器一致性套件（需 docker，真实 Redis Cluster）
```

## 状态与边界

开发中（alpha）。**不适合**：跨 slot 多行事务、大表任意 JOIN、强一致读、对 ~1ms 延迟税极端敏感的超热路径、不提供 EVAL 的 Redis 平台。完整边界见 [docs/14](docs/14-红线局限与检查单.md)。
