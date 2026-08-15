# 08 · 自治治理与热 Key

> **TiDB 参照**：桶分裂/合并 ↔ TiKV Region 分裂合并（阈值触发 + 在线迁移）；Controller ↔ PD 调度器 + DDL owner；
> 选举 ↔ `pkg/owner`（抢锁→任职→watchdog 盯续约→失约立即辞职）。
> 有意不抄：PD 的热点调度靠**搬 Region**——Redis Cluster 的 slot 搬迁由平台运维控制，KiDB 不主动迁移数据，
> 热点改用 L4 值复制摊开读（§8.4）。

## 8.1 Telemetry（双信号源）

**信号源 A（自研，默认）**：读写路径 1/64 概率采样，命中时在同 slot 统计 key `st:{桶}` 累加 ops（`HINCRBY`，越热发现越快）。

**信号源 B（可选能力，契约 [09](09-后端契约与适配器.md) §9.4）**：若适配器声明提供热 key 事件流（`HotkeyEvents() <-chan string`，部分企业级客户端内置热 key 检测上报），桶 key 被上报时直接触发 Controller 复核，作为信号源 A 的冗余与提前量。开源参考适配器不提供此能力——仅信号源 A 工作，功能无损。

**复核**：Controller 周期对采样热点桶执行 `ZCARD` + `MEMORY USAGE` 精确复核（阈值判断只信精确值，采样只负责发现）。

## 8.2 分裂/合并策略

| 触发 | 阈值（默认，配置热更新） |
|---|---|
| 分裂-成员 | > 50,000 |
| 分裂-体积 | > 8 MB |
| 分裂-读QPS | > 单节点安全值 40% |
| 分裂-等值倾斜 | 单值占桶 > 20% |
| 合并 | 成员 < 12,500 且 QPS < 阈值 1/4，持续 3 周期 |

滞后带（4×）防抖。范围桶分裂取 score 中位数（`ZRANGE` 搬迁时采样估算）；等值桶子桶数 ×2（pk xxhash64 高位掩码 +1 bit）；字典序副本按值字典序区间分裂。

**同分倾斜兜底（范围桶/字典序副本）**：当采样中位数 == 区间端点（海量成员 score 完全相同，如低基数字段被误建范围索引）时，score 维度裂不开，退化为**复合键分裂**——score 区间保持 `[K,K]` 不动，按 pk xxhash64 高位再分子桶（与等值桶同构）。查询侧对单点 score 追加子桶扇出，由回表校验兜底。该路径纳入 PBT P1 状态机不变式（[12](12-测试方案.md) §12.3）。

## 8.3 在线分裂协议（无停写、无读空洞、无 CROSSSLOT）

> **v1 实现状态**：Controller 选举 + watchdog 闭环已落地（controller 包，含故障迁移测试）；
> 本节的分裂/合并状态机（写路径 SPLITTING 双写 + 搬迁）是下一批——
> 落地前写路径一律 ACTIVE 单桶（桶超阈值暂由 key 体积监控告警兜底，功能正确性不受影响）。

子桶与父桶同 slot（stag 不变），全程单 slot Lua 原子步进（脚本资产见 [05](05-写入路径.md) §5.7）：

```
1. bucket_state_cas.lua：父桶置 SPLITTING，BucketMap version+1
   （HGET bm version 比对 ARGV 期望值，不符返回 stale）
2. write_row.lua 见 SPLITTING → 双写父桶+子桶（读仍只读父桶）
3. Controller 分批执行 split_migrate.lua（每批 500 成员，ants 池限速）：
   逐 member EXISTS 行key（已过期直接丢弃，顺路清理）→ ZADD 子桶 → ZREM 父桶
4. 搬迁完成 → 父桶 DRAINING，version+1 → 写入只写子桶，读走子桶
5. UNLINK 父桶 → 子桶 ACTIVE
```

- **中断恢复**：状态持久于 BucketMap，Controller 宕机后选主继任者断点续作；客户端旧版本 BucketMap 经 version 校验（写入 Lua 内 CAS）刷新重试；
- **合并**为镜像协议（MERGING：双写子桶+父桶 → 搬迁 → 只写父桶 → UNLINK 子桶）；
- **搬迁 Lua 的调用约束**：KEYS = {父桶, 子桶, 待判行key...} 全部同 slot，KEYS[1]=父桶（带 stag）作为路由依据（契约 R3）；
- 搬迁期间查询路径：ACTIVE/SPLITTING 读父桶，DRAINING 读子桶——由 BucketMap 版本保证读写双方看到一致状态，回表校验兜底一切中间态。

## 8.4 热 key 四层防御（读路径，逐级自动启用）

| 层 | 机制 | 实现 |
|---|---|---|
| L1 | 近缓存 | `golang-lru/arc/v2`（HashiCorp 官方 ARC，泛型）+ 自研 janitor 过期薄层（单一协程 + 最小堆，见下注）：谓词指纹→pk 列表 + BucketMap 缓存，TTL 3s；**全扫结果的指纹不入缓存**（ARC 自带抗扫描，此规则再减一分污染）；回表校验兜底陈旧 |
| L2 | 请求合并 | `singleflight` 合并同指纹并发查询 |
| L3 | 副本读 | 经 `Client.DoReplica`（可选能力，[09](09-后端契约与适配器.md) §9.4）：适配器把只读命令路由到 slave/只读集群。适配器不支持则该层自动关闭。回表校验兜底副本滞后 |
| L4 | 热桶值复制 | 超阈自动建 K=⌈热QPS/单节点安全QPS⌉（≤8）个**异 slot** 只读副本（副本 key = 源桶 key 的 stag 步进替换为 `SlotTag((源slot + k×1820) % 16384)`——确定性寻址、k∈[1,8] 互不撞 slot），随机读其一，1s 滚动刷新（`replica_refresh.lua`），热度回落自动 `UNLINK` 回收 |

L4 细节：

- 副本带 `@ver` 版本戳（Hash 副字段），读者校验，戳旧则回退源桶；
- 写路径不变只写源桶——无多副本写一致性问题；
- 刷新 = `ZRANGE 源桶` 全量读 + 副本 slot 内 Lua 原子重建（`DEL`+`ZADD`+`PEXPIRE 60s`），读者要么见旧副本要么见新副本；
- 触发信号：信号源 A（采样）或 B（热 key 事件流）任一命中 + Controller `ZCARD` 复核；
- **L4 解开了 Redis Cluster"单 key 无法水平扩展"的固有限制**：热桶读 QPS 摊到 K+1 个 slot。

**L1 janitor 过期薄层（ARC 无内置 TTL 的配套，~100 行自研）**：

- 结构：过期最小堆用 go-priorityqueue（选型表既有组件，与 top-k 归并共用依赖）；条目 value 携带 deadline；
- **职责分离**：过期正确性由读路径兜底（Get 时一次整数比较，消除 janitor 触发前的窗口）；**janitor 只负责主动释放内存**（周期清扫，间隔 min(TTL/4, 100ms)）；
- **防误删纪律（Replace 场景，两次评审钉死）**：Replace 会在堆里留新旧两条记录——janitor 弹出时比对"堆记录 deadline 与 ARC 当前条目 deadline，**相等才删**"；且**比对与删除在同一互斥临界区内**（`Add` 与摘除串行化），杜绝 Peek→Remove 之间的 TOCTOU 窗口误删新值。读路径（Get/Remove）不进临界区，热路径零影响；
- 生命周期随内核 Close 退出；单测覆盖：过期即删、Replace 不误删（假钟确定性回归）、并发对撞排空（-race）、janitor 退出无泄漏。

**L1–L4 的覆盖边界（诚实声明）**：四层防御保护的是**索引桶**，不直接保护单行读热 key（`d:{table}:{pk}` 的点查热点，如明星内容行）。行级热读的缓释手段：L3 副本读（`DoReplica` 把点查摊到 slave）+ 可选行级近缓存（L1 扩展：pk→行投影，3s TTL，回表校验天然保证不出错——默认关闭，由 `hotkey_row_cache` 变量开启）。行级**写**热点见 §8.6，属物理下界。

## 8.5 Controller 选举与 watchdog 闭环（移植 TiDB owner 语义）

> TiDB 参照：`pkg/owner/manager.go`——竞选成功只是开始，watchdog 盯 session，续约失败立即辞职再重新竞选。
> v5.0 补齐闭环：**锁续期失败必须当场停止干活**，消除"锁已丢但实例还在跑"的脑裂窗口。

```
循环：
  1. SET lk:ctrl token NX PX 10s        -- 抢锁（锁即选举）
  2. 抢到 → 进入 owner 态：启动分裂/合并控制循环 + DDL 作业巡检（[06](06-元数据与Schema演进.md) §6.3）
  3. 在任期间：watchdog 协程每 1/3 TTL 执行 lock_renew.lua 续约
     - 续约失败（锁丢/超时/网络断）→ 【立即】退出 owner 态，停止一切控制动作 → 回第 1 步
  4. 没抢到 → 跟随者态，周期重试（带抖动退避）
```

- Sweeper 的 slot 区间锁同理（每区间一把锁，抢到才清扫该区，续约失败立即停扫该区）；
- ReadWriteOnly 节点（引导配置豁免）不启动上述循环、不参与抢锁；
- 全部自治角色故障安全：全挂只会变慢/变浪费，不出错行（[01](01-定位架构与TiDB对齐.md) §1.7）。

## 8.6 写热点的出路

单 slot 写 ~10 万 ops/s 是物理下界。字段级写热点（如计数器类字段高频更新）：

1. 默认同步索引下，写入 Lua 同 slot 串行——热点行的写吞吐受单 slot 限制；
2. 开启**异步索引**（[05](05-写入路径.md) §5.2）：索引桶按 pk 全局散列，写热度摊平到全集群；
3. 行本身的写热点（同 pk 超高频写）超出缓存层职责，引导业务侧合并写/降频。

**两个方案自引入的单 key 写热点与对策**：

| 热点 key | 成因 | 对策 |
|---|---|---|
| `ver:{table}`（全局 INCR） | 早期设计每次写入 Lua 都 INCR 一次，写密集表（>5 万 wps）顶到单 slot 写上限 | **v5.0 起默认路径改行内 `HINCRBY _ver`（同 slot，免费）**；全局计数器仅低频聚合路径（维表刷新）按需聚合；若仍需全局单调源，分片计数器 `ver:{table}:{n}`（n=64，写入按 pk 选片） |
| `seq:{table}`（AUTO_INCREMENT INCR） | 同上，且语义要求近似单调 | 分片交错序列（shard i 发 i, i+n, i+2n...，同 MySQL `auto_increment_offset/increment` 语义——与 TiDB `AUTO_RANDOM` 同族的打散思想），仅开启 AUTO_INCREMENT 的表按需启用；默认单 key 已覆盖绝大多数场景 |
