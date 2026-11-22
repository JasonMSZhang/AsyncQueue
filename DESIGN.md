# 自研异步任务队列设计文档（AB‑Test 分桶计算）

> 技术栈：Java 17 / Spring Boot / spring-data-jpa / H2（file 持久模式）/ Caffeine
> 约束：不引入任何 MQ；队列基于 JDK `Lock + Condition` 自研有界优先级队列；`PriorityBlockingQueue` 不可作为实现主体。
> 部署形态：单实例（H2 嵌入式文件库不支持多写进程），无跨机容灾。

---

## 0. 总体结构与设计原则

```
        HTTP 提交
           │
           ▼
  ┌──────────────────┐   ①同步写库(事务提交)   ┌──────────────────────┐
  │  TaskSubmitter   │ ────────────────────▶ │  H2 file (t_task)    │
  └──────────────────┘                        └──────────────────────┘
           │ ②offer(成功/失败均可)                     ▲    │
           ▼                                          │    │ ④补偿扫描/重试扫描
  ┌──────────────────────────┐  ③take   ┌─────────────┴────┐
  │ BoundedPriorityQueue     │ ───────▶ │  Dispatcher(单线程)│
  │ heap + ReentrantLock     │          └─────────┬────────┘
  │ notEmpty / notFull       │                    │ execute
  └──────────────────────────┘                    ▼
                                     ┌───────────────────────────┐
                                     │ ThreadPool (N=16)         │
                                     │ AbBucketTaskHandler       │
                                     │  Caffeine(uid:expId,180s) │
                                     └───────────┬───────────────┘
                                                 │ ⑤结果+状态同事务提交
                                                 ▼
                                        t_task / t_task_result
```

三条贯穿全篇的原则：

1. **数据库是唯一事实来源**。内存队列、Caffeine、线程池中的在途任务都是「可丢弃的派生状态」，任一层丢失都能从 H2 重建。
2. **先持久化、后入队**。DB 事务提交成功即代表「任务不丢失」；入队只影响「多快被执行」，不影响「是否被记住」。
3. **至少一次执行 + 状态机 CAS**。以 `businessKey` 幂等 + 条件更新（`UPDATE ... WHERE id=? AND status=? AND version=?`）把 at‑least‑once 收敛为效果上的 exactly‑once。

### 0.1 非目标（明确不做）

- 不做多实例消费 / 分布式抢占（H2 文件库单写者，滚动发布必须保证同一时刻只有一个实例写同一库）。
- 不做任务 DAG / 依赖编排。
- 不做跨语言 hash 兼容之外的一致性协议（但 hash 实现必须锁定，见第 9 节）。
- 不做队列的「绝不重复执行」保证（代价过高），只做业务幂等。

---

## 1. 任务模型、状态枚举与表结构

### 1.1 领域对象

| 概念 | 说明 |
| --- | --- |
| `Task` | 一次业务计算的调度单元，只保存调度所需信息（类型、载荷、状态、重试、租约） |
| `TaskResult` | 任务成功后的业务产物，与 `Task` 1:1，独立成表，避免大字段污染主表 |
| `TaskPayload` | AB 任务入参：`uid`、`deviceId`、`expIds[]`、模拟参数（`sleepMs`、`failMode`） |
| `BusinessKey` | `md5(规范化标识 + ":" + 排序后的 expIds 拼接)`，用于幂等去重 |
| `Priority` | 0~9，数值越大越优先；重试任务降级为 1 |

### 1.2 状态枚举

| 状态 | 含义 | 是否终态 | 进入方式 | 可被扫描器重新投递 |
| --- | --- | --- | --- | --- |
| `PENDING` | 已持久化，等待被调度（在内存队列中，或等待补偿扫描） | 否 | 提交成功 / 重试到期 / 租约回收 / 停机回写 | 是 |
| `WAITING_RETRY` | 失败但可重试，已排定 `nextExecuteTime` | 否 | RUNNING 执行失败 | 是（到期后转 PENDING 再入队） |
| `RUNNING` | 已被某个 worker 持有（有 `leaseUntil`） | 否 | CAS PENDING→RUNNING | 是（租约过期视为崩溃回收） |
| `SUCCESS` | 计算成功且结果已落库 | 是 | RUNNING→SUCCESS | 否 |
| `FAILED` | 不可恢复，或重试次数耗尽 | 是 | RUNNING→FAILED | 否（仅人工重放） |
| `DEAD` | 死信，需人工介入（含崩溃次数超限） | 是 | RUNNING/PENDING→DEAD | 否（仅人工重放） |
| `CANCELED` | 主动取消（仅 PENDING/WAITING_RETRY 可取消） | 是 | PENDING→CANCELED | 否 |

为什么把 `WAITING_RETRY` 与 `PENDING` 分开：两者扫描 SQL、指标口径、运维动作完全不同。重试任务必须走独立的低频通道（见第 6 节），混在同一状态里会让「重试放大」不可观测。

### 1.3 状态机

```
                 ┌──────────┐  cancel   ┌──────────┐
   submit ──────▶│ PENDING  │──────────▶│ CANCELED │
                 └────┬─────┘           └──────────┘
                      │ CAS(PENDING→RUNNING)
                      ▼
                 ┌──────────┐  success  ┌─────────┐
                 │ RUNNING  │──────────▶│ SUCCESS │
                 └────┬─────┘           └─────────┘
       可重试失败 │    │ 不可恢复/耗尽         ▲
                 ▼    └──────────────────────┘
        ┌───────────────┐   ！保留「先写结果，再改状态」或同事务
        │ WAITING_RETRY │──到期──▶ PENDING
        └───────┬───────┘
                │ 超过最大次数 / 崩溃超限
                ▼
          ┌──────────┐        ┌──────┐
          │  FAILED  │        │ DEAD │
          └──────────┘        └──────┘
```

允许的「回退」只有两类，都通过条件更新完成，且必须携带 `version` 或 `status` 前置条件：

- `RUNNING → PENDING`：租约过期（宕机/线程被强制中断）。
- `FAILED/DEAD → PENDING`：人工重放接口（重置 `retryCount=0`，写审计日志）。

### 1.4 t_task（任务主表）

| 字段 | 类型 | 空 | 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `id` | varchar(32) | N | — | 主键，32 位无横线 UUID（服务端生成，不依赖自增，便于后续分片） |
| `business_key` | char(32) | N | — | md5 业务键，仅用于查询与去重判定，不唯一（终态后允许同键重提） |
| `active_key` | varchar(64) | Y | NULL | **活跃幂等键**：非终态时 = `business_key`，进入终态时置 NULL。唯一索引，靠它做并发去重 |
| `task_type` | varchar(32) | N | `AB_BUCKETING` | 任务类型，决定 handler 与异常分类策略 |
| `payload` | clob | N | — | JSON 载荷（uid/deviceId/expIds/模拟参数）；只读，不参与条件查询 |
| `status` | varchar(16) | N | `PENDING` | 状态枚举，字符串存储便于排查 |
| `priority` | int | N | 5 | 0~9，越大越优先 |
| `retry_count` | int | N | 0 | 已重试次数（不含首次执行） |
| `max_retry` | int | N | 3 | 该任务的重试上限，按异常类别可在提交时覆盖 |
| `next_execute_time` | timestamp | N | 提交时刻 | 最早可执行时间；退避即体现在此字段 |
| `lease_until` | timestamp | Y | NULL | 执行租约到期时间；`RUNNING` 时非空，正常路径不需要心跳 |
| `worker_id` | varchar(64) | Y | NULL | 持有者标识（实例 ID + 线程名 + 启动序号） |
| `version` | bigint | N | 0 | JPA `@Version` 乐观锁，兜住并发更新 |
| `error_code` | varchar(64) | Y | NULL | 最近一次失败的分类码（`BIZ_TIMEOUT` / `DB_LOCK` / `BAD_PAYLOAD` …） |
| `error_msg` | varchar(512) | Y | NULL | 截断后的异常摘要（完整栈进日志，不入库） |
| `created_at` | timestamp | N | now | 提交时间 |
| `updated_at` | timestamp | N | now | 状态更新时间，补偿扫描靠它做「刚提交任务」的宽限判断 |
| `finished_at` | timestamp | Y | NULL | 进入终态时间，用于耗时分位统计 |

索引设计：

| 索引 | 字段 | 用途 |
| --- | --- | --- |
| PK | `id` | 主键查询 |
| UK | `active_key` | 并发幂等去重（H2/SQL 标准下唯一索引允许多个 NULL，终态行互不影响） |
| IDX1 | (`status`, `next_execute_time`, `priority`) | 补偿扫描、重试扫描的主索引 |
| IDX2 | (`status`, `lease_until`) | 租约过期回收 |
| IDX3 | (`business_key`, `created_at`) | 幂等查询返回最近一次 taskId |

> 幂等实现要点：不要用 `business_key` 做唯一索引。需求是「未完成则复用」，终态后允许同键再次提交，唯一索引会让重提直接失败。`active_key` 方案把「活跃性」显式建模，冲突时捕获唯一键异常 → 回查返回既有 `taskId`，无需额外表或分布式锁。

### 1.5 t_task_result（结果表，与主表分离）

| 字段 | 类型 | 空 | 说明 |
| --- | --- | --- | --- |
| `task_id` | varchar(32) | N | 主键，同时是 `t_task.id` 的外键（1:1） |
| `result_json` | clob | N | 每实验一行结果：`expId`、`bucket`、`treatment/control`、`cacheHit` |
| `exp_count` | int | N | 结果条数，便于主表/接口快速校验完整性 |
| `hit_count` | int | N | 缓存命中数 |
| `miss_count` | int | N | 缓存未命中数 |
| `cost_ms` | int | N | 业务耗时分位统计来源 |
| `attempt_no` | int | N | 最终成功的是第几次尝试（1 表示首次成功） |
| `computed_at` | timestamp | N | 计算完成时间 |
| `created_at` | timestamp | N | 落库时间 |

分离的必要性：

1. **写放大隔离**：主表在任务生命周期内会被频繁更新（状态、租约、重试计数），把 CLOB 拆出去可让主表行保持小尺寸，H2 页缓存命中率更高，扫描器全表/范围扫描更快。
2. **读路径隔离**：任务列表/监控查询占绝大多数，不应该把结果大字段读进内存；JPA 侧不为两者建双向懒加载关系（避免 N+1），结果按 `taskId` 单独查询。
3. **生命周期不同**：结果只写一次、只读多次、可按月归档；主表需要长期保留调度痕迹。
4. **事务边界清晰**：成功路径在**同一事务**内「插入结果 + 更新状态为 SUCCESS」，避免出现「结果有了但状态没改」的中间态。

不保留每次失败尝试的结果行：失败不产生业务产物，尝试痕迹留在主表 `retry_count` / `error_code` 与日志中；如后续需要审计，再追加 `t_task_attempt` 历史表，不改变现有 1:1 语义。

### 1.6 提交时序（含幂等）

1. 参数校验与规范化（`uid` 去空白；`expIds` 去重 + 字典序排序；`uid` 缺失时使用 `deviceId` 并加前缀 `d:`，避免与 `u:` 同值混淆）。
2. 计算 `businessKey = md5(normalizedSubject + ":" + join(expIds, ","))`。
3. 尝试 `INSERT`（`active_key = businessKey`，`status = PENDING`）并提交事务；若触发 `active_key` 唯一键冲突 → 查询该活跃任务，**返回已有 taskId**（HTTP 200，`reused=true`）。
4. 入队：`offer(task, 100ms)`。
   - 成功：仅置内存标记（`inQueue=true`），**不额外写库**（减少写放大）。
   - 失败（队列满/超时）：记录背压指标，任务保持 `PENDING`，由补偿扫描器在下一轮重新投递。接口仍返回成功（`deferred=true`），因为「已持久化 = 已被接收」。
5. 返回 `taskId + status + deferred`。

---

## 2. 「入队不丢失」的持久化边界与宕机取舍

### 2.1 边界定义

**唯一的持久化边界 = H2 事务提交成功（commit 返回）。**

- 提交前崩溃：任务不存在，客户端可安全重试（无副作用）。
- 提交后崩溃：任务必然存在于 `t_task`，无论是否进过内存队列。
- 内存队列满、线程池拒绝、扫描器漏扫、Caffeine 全丢，都**不影响**「任务被记住」。

由此得到的部署约束：**接口一旦返回 taskId（202/200），就必须保证该 task 的 DB 事务已经提交**，绝不允许「先入队后异步落库」。

### 2.2 H2 层面的持久化强度（容易被忽略的风险点）

| 配置/行为 | 影响 | 结论 |
| --- | --- | --- |
| `WRITE_DELAY` 默认 500ms | 已 commit 的事务可能停留在内存缓冲，kill ‑9 / 断电会丢掉最近约 0.5s 的提交 | 若要求「返回即不丢」，必须显式设为 `WRITE_DELAY=0`（或 10~50ms），代价是写入吞吐下降、每次提交多一次 fsync |
| `DB_CLOSE_ON_EXIT=FALSE` | JVM 退出不自动关库，避免停机流程中被提前关闭 | 与优雅停机配合，最后显式 `SHUTDOWN` |
| MVStore 崩溃恢复 | 进程被杀后重启会做日志回放，已 fsync 的数据可恢复 | 依赖上一条：未 fsync 的数据无法恢复 |
| 单文件单写者 | 两个实例同时打开同一文件会失败或抢锁 | 部署必须单实例；滚动发布要求「先停旧、再起新」 |
| 磁盘满 / 文件损坏 | 无副本、无 WAL 归档 | 明确为**不可用级故障**，只能靠定期 `BACKUP TO` 与监控磁盘水位兜底 |

### 2.3 各崩溃点的处置

| 崩溃时刻 | DB 状态 | 是否丢失 | 恢复动作 |
| --- | --- | --- | --- |
| 校验/计算 businessKey 阶段 | 无记录 | 否（客户端可重试） | 无 |
| 事务已提交、尚未 offer | `PENDING` | 否 | 补偿扫描器（1s 周期）重新 offer；最多延迟 1 个扫描周期 + 宽限期 |
| 已 offer、worker 未取 | `PENDING` | 否 | 重启加载 `PENDING` 重新入队，重复入队由内存 `inQueue` 标记 + 队列元素去重吸收 |
| 执行中（计算完成前） | `RUNNING` + `lease_until` | 结果不丢（本来没有） | 重启后租约扫描把过期 `RUNNING` 改回 `PENDING` 重试；`crash_count` 累计，超过阈值转 `DEAD` 防止「毒任务」无限重启循环 |
| 结果写入与状态更新之间 | 同一事务，不存在该窗口 | 否 | — |
| 事务提交后、接口返回前 | `PENDING` | 否 | 客户端可能重试，幂等命中返回同一 taskId |
| H2 未 fsync 窗口内被杀 | 记录可能不存在 | **可能丢**（默认配置） | 只能通过 `WRITE_DELAY=0` 消除；本节 2.2 已说明代价 |

### 2.4 优雅停机 vs kill ‑9 的取舍

| 维度 | 优雅停机 | kill ‑9 / 断电 |
| --- | --- | --- |
| 已提交未执行的任务 | 内存队列剩余元素回写为 `PENDING`，下一次启动即可执行 | 依赖 DB，天然可恢复，无需回写 |
| 执行中的任务 | 线程池 `awaitTermination` 等待完成，或超时后中断并回写 `PENDING` | 丢失执行进度，靠租约过期回收重跑 |
| 重复执行 | 有控制，通常 0 次重复 | 必然出现重复执行（AB 计算无副作用，重跑安全） |
| 状态一致性 | 停机流程保证不出现「永远 RUNNING」 | 可能出现 `RUNNING` 僵尸行，需租约扫描清理 |
| 恢复时延 | 秒级 | 依赖 H2 恢复 + 扫描周期，约数十秒 |
| 代价 | 停机耗时受最长任务与超时预算约束（建议 ≤ 30s） | 无停机延迟，但需接受重跑与状态清理成本 |

结论：**kill ‑9 的代价被设计成了「可接受的重复执行」**，而不是「数据丢失」。这个结论成立的前提有三条，缺一不可：

1. 先写库后入队（第 2.1 节）；
2. 任务副作用幂等（AB 分桶是纯计算，天然幂等；若未来接入有副作用的 handler，必须由 handler 自己做幂等）；
3. 有机制回收僵尸 `RUNNING`：当前实现利用**单实例**前提，在启动时把所有 `PROCESSING` 重置为 `PENDING` 后重新加载；运行期不退出、只线程假死的场景仍需租约/心跳（见 README 已知限制）。

> 实现补充：H2 文件库 URL 必须带 `DB_CLOSE_ON_EXIT=FALSE`。否则 JVM 收到 SIGTERM 时，H2 自身的 shutdown hook 会与 Spring 优雅停机并发执行并提前关闭数据库，导致队列回写与在途任务状态回写失败（实测出现过 `Database is already closed`）。

---

## 3. 内存有界优先级队列：容量决策与背压

### 3.1 实现形态（约束落地）

- 内部为**数组实现的二叉堆**，`Object[]` + `size`，不借助 `PriorityBlockingQueue`。
- 一把 `ReentrantLock`，两个 `Condition`：`notEmpty`（消费者等待）、`notFull`（生产者在阻塞模式下等待）。
- `offer(item, timeout)`：持锁判断容量 → 满则 `notFull.awaitNanos(...)` → 超时返回 `false`；入堆后 `siftUp` 并 `notEmpty.signal()`。
- `take()`：持锁循环等待非空 → 取堆顶 → 用末尾元素覆盖并 `siftDown` → `notFull.signalAll()`。
- 优先级比较器：`priority` 降序，同优先级按 `seq`（单调递增 long）升序，保证 FIFO 且避免比较器不一致导致的堆退化。
- 元素只持有 `taskId + priority + seq + inQueue 原子标记`，**不持有 payload / 实体**，避免内存膨胀与脏读。
- 关闭语义：`closed` 标志 + 唤醒全部等待者，`offer` 立即失败，`take` 返回空，剩余元素交给停机流程回写。

### 3.2 容量决策依据

容量不是拍脑袋的数字，由三个约束夹逼取最小值：

| 约束 | 计算 | 结果 |
| --- | --- | --- |
| 时延约束（主要） | `容量 ≈ 峰值 QPS × 可接受排队时延`。峰值 300 tps × 5s = 1500 | 1500 |
| 吞吐匹配 | 单任务均值 80ms、16 线程 → ≈ 200 任务/s 的处理能力；队列需覆盖 1.5~2 倍峰值的短时突发 | 1800~2400 |
| 内存约束 | 单个队列元素 ≈ 100~200B，4096 个 < 1MB（可忽略）；但队列越大，DB 中 `PENDING` 堆积越多，恢复与排查成本越高 | 上限 8192 |
| 下界 | 不小于 `2 × 线程数`，否则调度抖动会让线程空转 | 32 |

**取值：4096**（取 2 的幂，便于后续换成分段锁/环形结构时不改配置语义）。

配套决策：

- 容量外置为配置项（`queue.capacity`），支持压测时调整，但默认值按上述推导锁死。
- 队列长度本身是**背压信号**，`usage > 80%` 触发告警；`usage` 长期 > 90% 说明线程池或 DB 写能力已成为瓶颈，应先扩容写路径而不是加队列。
- 队列满不代表任务丢失（已在库），因此**可以毫不犹豫地拒绝入队**，这是本设计最重要的一条「松耦合」。

### 3.3 背压策略选型

| 方案 | 做法 | 优点 | 缺点 | 结论 |
| --- | --- | --- | --- | --- |
| A. 阻塞生产者 | `offer` 无限/长超时等待 `notFull` | 实现最简单，天然限速 | 占满 Tomcat 工作线程，H2 单写者下会连带拖死查询接口；超时语义难解释 | 不采用 |
| B. 快速失败 + DB 兜底 | 短等待（100ms）后失败，任务留在 `PENDING`，由补偿扫描重投 | 不阻塞请求线程；任务不丢；实现简单 | 执行时延不可控（最坏多 1 个扫描周期）；需要扫描器 | **采用（主策略）** |
| C. 丢弃最旧 / 最低优先级 | 驱逐队头或最低优先级元素 | 保持切入队任务时延 | 被驱逐任务只是延后执行，无收益；破坏优先级语义 | 不采用 |
| D. 准入限流（信号量/令牌桶） | 提交前按「未完成任务数」或 QPS 限流 | 把压力挡在 DB 写入前，保护 H2 | 需要额外计数与清理（任务终态时释放许可）；被拒请求需客户端配合 | **采用（前置层，与 B 组合）** |

最终背压链路（三段式，压力逐级向上传导且都不丢任务）：

```
执行层满(池队列 64 + 16 线程)
   └─ CallerRunsPolicy → Dispatcher 变慢（不丢、不堆积）
        └─ 内存队列不再出队 → 使用率上升至 100%
             └─ offer 失败 → 标记 deferred + 指标打点 → 接口快速返回（任务仍在 DB）
                  └─ Semaphore 许可耗尽 → 新请求直接 429（可选前置保护）
```

为什么短等待 100ms 而不是 0 或无限：0 会让「瞬时抖动」（一次 GC、一次 DB 慢写）直接转成 deferred，放大扫描器压力；无限则退化成方案 A。100ms 恰好覆盖调度抖动，同时对 p99 请求时延影响可控。

---

## 4. 线程池参数选型

### 4.1 任务画像

| 阶段 | 耗时量级 | 性质 |
| --- | --- | --- |
| payload 解析 + 10 次 Caffeine 查询 | 10~50 μs（命中） | CPU + 本地内存 |
| 未命中时 Murmur3 × 10 | < 10 μs | 纯 CPU |
| 模拟业务 sleep | 20~200ms（可配置） | IO 等待（占绝对主导） |
| CAS 置 RUNNING + 写结果 + 改状态 | 5~20ms | DB IO（H2 单写者，存在串行点） |

结论：这是**等待占比很高的混合任务**，但真实瓶颈是 H2 的单写线程，而不是 CPU。线程数超过某个点后，增加线程只会加剧写锁排队与上下文切换。

### 4.2 参数

| 参数 | 取值 | 依据 |
| --- | --- | --- |
| `corePoolSize = maxPoolSize` | 16（8 核机器） | `N = CPU × (1 + W/C)`，W/C ≈ 1（sleep 与计算各半）→ 16；再高会压垮 H2 写路径 |
| 队列 | `ArrayBlockingQueue(64)` | 只做线程间削峰，真正的缓冲在有界优先级队列；64 ≈ 4 × 线程数 |
| 拒绝策略 | `CallerRunsPolicy` | 让 Dispatcher 亲自执行，形成「执行层 → 调度层 → 队列层」的背压传导，不丢任务、不无限堆积 |
| `allowCoreThreadTimeOut` | false | 固定线程数，避免流量低谷后突增的建线程抖动 |
| `prestartAllCoreThreads` | true | 启动即预热，减少冷启动首批任务时延 |
| 线程工厂 | 非守护线程 + 命名 `ab-worker-%02d` | 优雅停机需要非守护线程配合 `awaitTermination`；命名便于 jstack 与指标定位 |
| `awaitTermination` | 20s | 覆盖 p99 任务耗时（约 1s 级）加安全余量；超出即中断并回写 |
| 异常处理器 | `UncaughtExceptionHandler` 兜底 | 防止 handler 漏捕获导致任务永远 `RUNNING` |
| Hikari `maximumPoolSize` | 12~16 | 与 worker 数匹配但略小，避免连接数超过 H2 写能力；`connectionTimeout=3s`，拿不到连接按系统异常退避重试 |

### 4.3 为什么不采用的其他方案

- `newCachedThreadPool`：无界线程 → H2 写锁排队、线程栈内存暴涨、停机时无法在预算内排空。
- 大 `maxPoolSize` + 大队列：掩盖真实瓶颈，表现为「任务都在排队、监控看着健康、实际 p99 持续恶化」。
- `SynchronousQueue`（零缓冲）：一次 `execute` 失败就要走拒绝分支，调度抖动直接转成 deferred，放大扫描器压力；64 的缓冲是刻意的折中。
- 虚拟线程（Java 21+ 才有）：对 sleep 型任务收益最大，但本项目锁定 Java 17，且 H2 写瓶颈不会因虚拟线程消失，故不引入。

### 4.4 双队列的关系（不要混淆）

- **优先级队列**：决定「谁先被执行」，容量 4096，跨任务类型的公平性与优先级都在这里体现。
- **线程池队列**：决定「已经派发出去、线程暂时没空」的缓冲，容量 64，只做削峰，不承担优先级语义。
- **Dispatcher 单线程**：唯一从优先级队列取任务的角色，保证优先级严格生效；若让多个 worker 直接 `take`，池内 FIFO 会抹平优先级。

---

## 5. 异常分类与处理

### 5.1 三类异常

| 类别 | 典型异常 | 判定依据 | 处理 |
| --- | --- | --- | --- |
| **业务可重试** `BIZ_RETRYABLE` | 分桶配置暂未加载、配置版本刷新中、依赖服务 5xx/超时、限流拒绝、乐观锁冲突 | 有明确错误码或可识别的业务异常类型；重试大概率成功 | 指数退避重试，`maxRetry=3` |
| **系统可恢复** `SYS_RECOVERABLE` | H2 锁超时/死锁、连接池获取超时、磁盘瞬时 IO 错误、线程池拒绝、`OutOfMemoryError` 之外的资源不足 | 基础设施瞬时故障；重试可能成功但需要更长间隔 | 更长退避重试，`maxRetry=5`，并触发熔断计数 |
| **不可恢复** `UNRECOVERABLE` | payload 无法解析、`expId` 不存在、`uid` 与 `deviceId` 同时为空、配置数据非法、`NoSuchMethod`/`ClassCast`、确定性计算错误 | 相同输入重试必然同样失败 | **不重试**，直接 `FAILED`（毒任务可转 `DEAD`），告警 |
| 崩溃回收 `CRASH_RECOVERED` | 租约过期、实例被 kill | 由扫描器而非异常处理器判定 | 计入 `crash_count`，`≤ 2` 次转 `PENDING`，超过转 `DEAD` |

### 5.2 关键设计点

1. **分类在 handler 边界完成**：handler 只抛异常，由 `ExceptionClassifier` 统一映射为 `RetryDecision{RETRY, FAIL, DEAD}`，避免每个业务 handler 自己写重试逻辑。
2. **未知异常默认按 `SYS_RECOVERABLE` 处理**（保守假设：可能是基础设施问题），但只有一次机会升级判定——若同一任务连续两次因未知异常失败，第 3 次直接 `FAILED`，避免用重试掩盖 bug。
3. **`Error` 与 `Exception` 分开**：`OutOfMemoryError`、`StackOverflowError` 不重试，直接 `DEAD` 并让实例进入不健康状态（重启比带病重试更安全）。
4. **DB 写失败是最危险的一类**：执行成功但结果落库失败时，不能把任务标 `SUCCESS`。此时事务回滚，任务保持 `RUNNING`，由租约过期回收重跑（AB 计算幂等，重跑安全）。
5. **异常信息截断入库**：`error_msg` 只存 512 字符摘要，完整栈进日志（带 taskId），避免 CLOB 膨胀与敏感信息落库。
6. **中断（`InterruptedException`）单独处理**：属于停机信号，不消耗重试次数，直接把任务回写 `PENDING` 并恢复中断标志。

---

## 6. 重试策略与死信

### 6.1 参数

| 类别 | 最大重试 | 基础间隔 | 退避因子 | 上限 | 抖动 |
| --- | --- | --- | --- | --- | --- |
| `BIZ_RETRYABLE` | 3 | 1s | 2 | 30s | Full Jitter |
| `SYS_RECOVERABLE` | 5 | 2s | 2 | 60s | Full Jitter |
| `UNRECOVERABLE` | 0 | — | — | — | — |
| `CRASH_RECOVERED` | 2 | 5s | 1（固定） | 5s | 固定 + 小抖动 |

延迟公式：`delay = random(0, min(cap, base × 2^attempt))`（Full Jitter）。首次重试即可能在 0~1s 内触发，主要用于快速吸收瞬时抖动；后续退避快速拉开，避免同一批失败任务「时钟同步」式同时回归。

### 6.2 重试流程（硬性约束落地）

```
worker 失败
  └─ classifier → RETRY
       └─ 条件更新：RUNNING → WAITING_RETRY
            retry_count += 1
            next_execute_time = now + delay
            lease_until = NULL, worker_id = NULL
       └─ 不 offer 回内存队列  ← 关键约束
            │
RetryScheduler（独立单线程，1s 周期，每次最多 200 条）
  └─ SELECT ... WHERE status='WAITING_RETRY' AND next_execute_time <= now
                  AND retry_count <= max_retry
       ORDER BY next_execute_time LIMIT 200
  └─ 达标者 CAS 改为 PENDING（或直接带低优先级 offer）
```

- 首次任务与重试任务共享同一张表，但**不同扫描通道、不同优先级**（重试 `priority = 1`），保证首次任务优先被消费。
- 扫描器必须**限批量 + 限速**：一次 200 条、每秒 1 轮，重试洪峰被硬性摊平；配合队列剩余容量做二次校验，队列使用率 > 80% 时本轮跳过（重试让位于正常流量）。
- 重试计数与上限比较放在 SQL 条件里（`retry_count < max_retry`），避免并发重复投递时超发。

### 6.3 死信机制

- 触发条件：重试耗尽、不可恢复、崩溃次数超限、人工标记。
- 落地：`status=DEAD` + `error_code`，不额外建表（避免双写一致性），查询用 `status='DEAD'` 索引。
- 人工重放接口：校验 → 重置 `retry_count=0`、`next_execute_time=now`、`status=PENDING`、`active_key` 回填（若业务键仍无活跃任务），记录操作人审计日志。
- 死信必须配告警：`DEAD` 新增 > 0 即告警，否则等于静默丢任务。

### 6.4 避免「重试放大」队列压力

| 手段 | 作用 |
| --- | --- |
| 重试只写库、不回内存队列 | 重试流量与在线流量在调度层彻底解耦（硬约束） |
| 独立低频扫描器 + 批量上限 | 把重试洪峰摊平到多个扫描周期 |
| 全抖动退避 | 打散「同时失败 → 同时重试」的同步波 |
| 重试任务优先级 1 | 正常任务永远优先，重试不会挤占首提任务 |
| 重试流量预算 | 重试入队数 / 总入队数 > 20% 时暂停本轮重试投递并告警 |
| 按类型熔断 | 同一 `task_type` 在 30s 内失败 > 20 次 → 暂停投递该类型 30s，防止缺陷版本全量重试 |
| 队列水位联动 | 内存队列使用率 > 80% 时跳过重试扫描 |
| 崩溃上限 | 毒任务不会在重启循环中无限重跑 |

---

## 7. 完整优雅停机流程

使用 `SmartLifecycle`（或 `@PreDestroy` + 显式阶段编排）按顺序执行，**顺序不可交换**：先停生产、再停消费、最后停存储。

| 阶段 | 动作 | 超时 | 关键点 |
| --- | --- | --- | --- |
| T0 摘流 | 健康检查置 `DOWN`，等待 LB/K8s 摘除 | 5s（`terminationGracePeriodSeconds` ≥ 30s） | 必须早于关队列，否则仍有新请求打进来 |
| T1 停接入 | `accepting = false`，提交接口立即返回 503；**已提交事务不受影响** | — | 此阶段不再产生新的 `PENDING` |
| T2 关队列入口 | `queue.close()`：置 `closed`，`notEmpty.signalAll()` 唤醒所有等待者 | 立即 | 之后 `offer` 直接失败，不再接受新元素 |
| T3 停 Dispatcher | 中断调度线程 / 让其自然退出，不再向线程池派发 | 2s | 必须先停派发，否则 `shutdown()` 后仍会收到 `RejectedExecutionException` |
| T4 排空线程池 | `shutdown()` + `awaitTermination(20s)`，队列继续被消费，在途任务跑完 | 20s | 不调用 `shutdownNow()`，先给任务完成机会 |
| T5 强制回收 | 超时未完成：`shutdownNow()` 中断；被中断任务捕获 `InterruptedException`，**不消耗重试次数** | 2s | 中断后仍需等 worker 退出，最多再等 3s |
| T6 状态回写 | ① 对 `worker_id = 本实例` 且仍 `RUNNING` 的任务条件更新为 `PENDING`；② 内存队列剩余元素对应的任务确保为 `PENDING`（多数本来就已是），清空 `inQueue` 标记 | 5s | 用「`worker_id = 本实例`」做精确限定，绝不按状态全表扫 |
| T7 停调度器 | 关闭补偿扫描器、重试扫描器、租约回收器、指标上报线程 | 1s | 必须在 T6 之后，避免扫描器把刚回写的任务又捞走 |
| T8 关缓存 | `Caffeine.invalidateAll()` / 直接丢弃 | — | 缓存是派生数据，无需持久化 |
| T9 关持久层 | 关闭 JPA `EntityManagerFactory` → 关闭 Hikari → 执行 H2 `SHUTDOWN`（因 `DB_CLOSE_ON_EXIT=FALSE`，必须显式关闭，否则 MVStore 文件可能未完整落盘） | 5s | **最后一步**；顺序颠倒会出现在途事务写失败 |
| T10 退出 | 打印停机摘要（耗时、回写条数、未完成任务数） | — | 便于运维判断是否需要人工介入 |

总预算 ≈ 30s。若 `terminationGracePeriodSeconds` 小于该预算，`SIGKILL` 会在中途打断，此时依赖第 2.4 节的租约回收兜底——**停机流程是优化，不是正确性的唯一保证**。

几个易错点：

- 线程池必须用非守护线程，否则 JVM 可能在 `awaitTermination` 前就开始退出。
- `T4` 与 `T6` 之间不能有「新任务入队」，否则会回写出不该回写的任务（靠 T2/T3 保证）。
- 停机过程中禁止触发重试逻辑（`WAITING_RETRY` 也不该在停机阶段投递），否则会与 `T7` 抢状态。
- 停机耗时要打点，作为发布流水线的观测项；若长期接近预算上限，说明 p99 任务耗时已超标。

---

## 8. 可观测指标清单

统一用 Micrometer 打点（Actuator `/actuator/prometheus`），标签保持一致（`task_type`、`status`、`exception_class`、`priority`），所有日志带 `taskId` + `traceId`。

### 8.1 队列层

| 指标 | 类型 | 说明 / 告警 |
| --- | --- | --- |
| `queue.size` / `queue.capacity` / `queue.usage` | Gauge | 使用率 > 80% 告警 |
| `queue.offer.total{result=ok/full/timeout/closed}` | Counter | `full` 速率突增 = 下游处理不过来 |
| `queue.offer.wait.duration` | Timer (p50/p95/p99) | 反映锁竞争与背压程度 |
| `queue.take.wait.duration` | Timer | 消费者空等时长，长期为 0 说明吞吐饱和 |
| `queue.deferred.total` | Counter | 因队列满延后执行的任务数（核心背压指标） |
| `queue.reenqueue.dedup.total` | Counter | 重复入队被去重吸收的次数（应稳定小幅） |

### 8.2 执行层

| 指标 | 类型 | 说明 |
| --- | --- | --- |
| `task.exec.total{status=success/failed/dead}` | Counter | 成功率与三类终态分布 |
| `task.exec.duration` | Timer (p50/p99) | 与 sleep 配置对比，识别异常任务 |
| `task.wait.duration` | Timer | 提交 → 开始执行，端到端排队时延（最关键的用户体感指标） |
| `executor.active` / `executor.queued` / `executor.pool.size` | Gauge | 线程饱和度；`queued` 长期 > 0 说明线程不足 |
| `executor.rejected.total` | Counter | 理论上很少（CallerRuns 会先兜住），出现即异常 |

### 8.3 重试与死信

| 指标 | 类型 | 说明 |
| --- | --- | --- |
| `task.retry.total{exception_class}` | Counter | 按异常分类拆解，定位主要失败源 |
| `task.retry.exhausted.total` | Counter | 重试耗尽数，> 0 需关注 |
| `deadletter.size` | Gauge | 死信堆积，新增即告警 |
| `task.retry.ratio` | Gauge | 重试入队 / 总入队，> 20% 触发限流 |
| `scheduler.retry.scan.duration` / `scheduler.retry.candidates` | Timer / Gauge | 扫描是否成为瓶颈（candidates 长期 = 200 说明批量不够） |

### 8.4 持久化层

| 指标 | 类型 | 说明 |
| --- | --- | --- |
| `db.task.update.duration` | Timer | 状态 CAS 的耗时；H2 写锁竞争的直接体现 |
| `db.pending.backlog` | Gauge | `PENDING` 且 `next_execute_time <= now` 的堆积量，> 队列容量 2 倍需扩容 |
| `db.running.expired` | Gauge | 僵尸 `RUNNING` 数量，出现即说明租约回收在工作 |
| `lease.recovery.total` / `crash.count.max` | Counter / Gauge | 崩溃回收次数，单任务崩溃次数接近上限要人工介入 |
| Hikari `active/idle/pending/timeout` | Gauge/Counter | 连接池耗尽是重试放大的常见根因 |
| H2 文件大小 / 磁盘可用空间 | Gauge | 磁盘 < 20% 告警（H2 无副本，满盘即不可用） |

### 8.5 缓存与业务

| 指标 | 类型 | 说明 |
| --- | --- | --- |
| `cache.hit.ratio`（Caffeine `recordStats`） | Gauge | 压测需构造重复 uid 才能打出真实命中率，轮询唯一 uid 时命中率≈0 属正常 |
| `cache.size` / `cache.eviction.total` | Gauge / Counter | 逼近 `maximumSize` 说明淘汰频繁、容量需评估 |
| `cache.load.duration` | Timer | 未命中时的计算耗时（μs 级，异常升高说明实现退化） |
| `ab.bucket.count{exp_id,bucket_group}` | Counter | 桶分布直方图，用于在线校验均匀性 |
| `ab.treatment.ratio{exp_id}` | Gauge | 实验组实际占比 vs 配置占比，偏差 > 0.5% 告警 |
| `ab.result.exp.count.mismatch` | Counter | 结果条数 ≠ 请求 expIds 数，应恒为 0 |

### 8.6 端点与日志

- 自建只读运维端点：`/ops/queue`（容量/水位/优先级分布）、`/ops/task/{id}`（状态+错误码+重试次数）、`/ops/deadletter`（列表+重放）。
- 关键日志事件：`TASK_SUBMITTED`、`TASK_REUSED`、`TASK_STARTED`、`TASK_SUCCEEDED`、`TASK_RETRY_SCHEDULED`、`TASK_DEAD`、`QUEUE_REJECTED`、`LEASE_RECOVERED`、`SHUTDOWN_PHASE{n}`。

---

## 9. AB‑Test 业务实现要点

### 9.1 Hash 与分桶

- **key 规范**：`{subject}:{expId}`，其中 `subject` 优先 `uid`，缺失时用 `d:{deviceId}`（前缀化，避免与真实 uid 撞值）。`expId` 固定小写、去空白。
- **哈希算法**：MurmurHash3。**必须在文档与代码中锁定到唯一实现**（推荐 `murmur3_32(seed=0)`，输出 32 位），否则 `murmur3_128` 取高半区/低半区、大小端不同会导致同一用户在不同端落到不同桶。这是跨语言分桶最常见的线上事故来源。
- **取桶**：`bucket = Math.floorMod(hash32, 10000)`（`floorMod` 而非 `%`，确保负 hash 也映射到 0~9999，是必须显式处理的边界）。
- **判定**：`bucket < bucketLimit(expId)` → `treatment`，否则 `control`。`bucketLimit = floor(10000 × 实验组占比)`：test1=500、test2=1500 … test10=9500。
- **边界值必须单测**：`hash % 10000 == 0`、`== 499`（test1 组内最后一个桶）、`== 500`（control 第一个桶）、`== 9999`。

### 9.2 正交与均匀性

- **正交来源**：key 中拼接了 `expId`，10 个实验的哈希输入互不相同，输出近似独立，因此同一用户在 test1 实验组、test10 对照组同时出现的概率 ≈ 5% × 95%。**不要**用「同一 uid 哈希 + 实验偏移量」的实现方式——那会引入确定性相关，破坏正交。
- **均匀性**：10000 桶对 32 位哈希足够（每桶约占 1/10000，标准误 ≈ 0.01%）。用卡方检验验证 10 个区间的均匀性；线上用 `ab.bucket.count` 看分布是否偏移。
- **抽样校验方法**：离线取 100 万 uid，计算 test1 × test10 的 2×2 交叉表，期望各格 ≈ 5%×95%×N，偏差超过 3σ 视为分布异常。
- **一致性要求**：同一 `uid` 在不同设备、不同时间必须得到相同结果——这由 key 规范性保证，不依赖缓存。

### 9.3 Caffeine 缓存策略

| 项 | 取值 | 理由 |
| --- | --- | --- |
| key | `uid:expId`（与 hash key 完全一致） | 一实验一项，配置变更/单实验灰度可精确失效；批量 10 个实验时用 `getAll` 一次批量取，避免 10 次独立查询 |
| value | 不可变结果对象：`bucket`、`treatment`、`expId`、`configVersion`、`computedAt` | 不可变避免并发脏读；带 version 便于观测配置生效 |
| TTL | `expireAfterWrite(180s)` | 与需求的 3 分钟一致。语义是「最多 3 分钟后重新计算」，不是「最多 3 分钟后失效」 |
| 容量 | `maximumSize(200_000)` | 单项约 150~250B（含 key、对象头、Caffeine 节点开销）→ 约 30~50MB，属于可控范围；按「活跃 uid × 实验数」估算，若单机活跃用户 2 万 × 10 实验 = 20 万 |
| 统计 | `recordStats()` | 命中率、加载耗时、淘汰数上指标 |
| 失效 | 配置变更时 `invalidateAll()`（或按 expId 定向失效） | 避免「改配置后 3 分钟内结果不一致」；用 expId 批量失效更平滑 |
| 负缓存 | 不缓存非法入参 | 非法 uid 属于不可恢复异常，缓存会让问题被掩盖；若需抗穿透，单独 30s 空值缓存并打点 |
| 并发加载 | 依赖 Caffeine 的单 key 串行加载 | 天然防止同 key 惊群；服务层不得在缓存外层再加锁（会退化成串行） |

要点说明：

- **缓存收益有限但必须有**：Murmur3 是 μs 级计算，缓存真正省的是「重复 hash + 模拟 IO」，并承担压测中「缓存 IO 混合」的建模。因此命中率低不代表设计失败，但**不允许因为命中率低就去掉 TTL**（需求要求 3 分钟）。
- **TTL 与配置变更的取舍**：`expireAfterWrite` 会让配置变更最长滞后 180s。若业务要求秒级生效，必须走「主动失效 + 配置版本号」路线，而不是缩短 TTL（缩短 TTL 会提高缓存未命中率与 CPU 开销）。
- **缓存只在 worker 线程内使用**：不在提交接口里预热（会把慢计算带回请求线程），也不在 Dispatcher 里访问（避免拖慢调度）。
- **结果里记录 `cacheHit`**：既是业务返回的一部分，也是核对缓存实际生效的抓手（`hit_count + miss_count == exp_count`）。

### 9.4 幂等业务键

- 计算：`businessKey = md5(subject + ":" + join(sortedExpIds, ","))`。
- 必须排序 `expIds`：`[test1,test2]` 与 `[test2,test1]` 是同一业务请求，不排序会产生两个 taskId。
- `subject` 必须与 hash key 的规范一致（`u:` / `d:` 前缀），否则「同一用户不同入参形态」会被判成不同任务。
- 幂等只覆盖「未完成」：任务进入 `SUCCESS/FAILED/DEAD/CANCELED` 后 `active_key` 置 NULL，同键可再次提交（例如用户主动要求重算）。
- 返回既有任务时需带 `status`，客户端据此决定轮询还是直接取结果。

---

## 10. 参数汇总与风险清单

### 10.1 关键参数

| 参数 | 默认值 | 备注 |
| --- | --- | --- |
| `queue.capacity` | 4096 | 见 3.2 推导 |
| `queue.offer.timeout` | 100ms | 兼顾抖动吸收与请求时延 |
| `worker.pool.size` | 16 | 8 核机器 |
| `worker.pool.queue` | 64 | 仅削峰 |
| `task.maxRetry.biz` | 3 | 业务可重试 |
| `task.maxRetry.sys` | 5 | 系统可恢复 |
| `task.retry.baseDelay` | 1s / 2s | 按类别 |
| `task.retry.cap` | 30s / 60s | 按类别 |
| `task.lease.duration` | 60s（≥ 3 × p99 任务耗时） | 租约过期即回收 |
| `scheduler.compensate.interval` | 1s | `PENDING` 回补 |
| `scheduler.retry.interval` | 1s，批量 200 | 重试投递 |
| `scheduler.lease.interval` | 5s | 僵尸 RUNNING 回收 |
| `task.crash.maxRecover` | 2 | 超过转 DEAD |
| `cache.ttl` | 180s | `expireAfterWrite` |
| `cache.maxSize` | 200_000 | 约 30~50MB |
| `jdbc WRITE_DELAY` | 0 | 严格「返回即不丢」；若可接受丢 0.5s 可调回 500 换吞吐 |
| 停机总预算 | 30s | 需 ≤ `terminationGracePeriodSeconds` |

### 10.2 风险与缓解

| 风险 | 影响 | 缓解 |
| --- | --- | --- |
| H2 单写者是真实瓶颈 | 线程数/队列再大也无法提升吞吐 | 优先优化写路径（短事务、批量提交、结果 CLOB 分离）；监控 `db.task.update.duration` |
| 单实例、单文件 | 无高可用、无法水平扩展 | 明确为非目标；滚动发布保证单写者；定期 `BACKUP TO` |
| `kill -9` 导致重复执行 | 结果重复计算（无副作用则无害） | 业务幂等 + 结果表 1:1 upsert + 状态 CAS |
| 租约时长设置不当 | 过短导致长任务被重复执行；过长导致僵尸任务恢复慢 | `lease = 3 × p99`；任务内部不做长事务 |
| 补偿扫描与提交竞态 | 刚提交的任务被重复入队 | `updated_at` 宽限期 2s + 内存 `inQueue` 原子标记 + 执行前 CAS |
| 重试风暴 | 队列被重试占满，正常任务饿死 | 独立扫描通道 + 优先级降级 + 比例预算 + 熔断 |
| 缓存 TTL 导致配置延迟生效 | 最长 180s 结果与配置不一致 | 配置变更主动失效；结果带 `configVersion` |
| 幂等键规范化不一致 | 同一业务请求生成两个任务 | `subject` 规范化 + `expIds` 排序，单测覆盖 |
| Murmur3 实现不一致 | 跨端分桶漂移 | 锁定算法/seed/取位/字节序，并提供固定测试向量 |

### 10.3 建议的验收场景

1. **正常提交**：1000 个不同 uid，全部 `SUCCESS`，10 个实验各有一条结果，桶号在 0~9999。
2. **幂等**：同 uid 同 expIds 乱序提交两次，返回同一 taskId；等任务 SUCCESS 后再提交，得到新 taskId。
3. **业务异常重试**：开启 failMode，前 2 次抛业务异常，第 3 次成功；`retry_count=2`，`attempt_no=3`。
4. **不可恢复异常**：非法 payload 直接 `FAILED`，`retry_count=0`。
5. **队列打满**：压测超过 4096 待执行任务，观察 `queue.deferred.total` 上升，最终全部执行成功（无丢失）。
6. **DB 短暂不可用**：任务停在 `WAITING_RETRY`，恢复后自动完成。
7. **kill ‑9**：执行中强杀，重启后 `PENDING` 任务全部完成，`RUNNING` 僵尸被回收，崩溃次数 ≤ 2。
8. **优雅停机**：停机期间提交请求返回 503，在途任务完成，重启后无遗留 `RUNNING`。
9. **缓存**：重复 uid 循环 10 万次，命中率应显著高于 0；唯一 uid 压测命中率 ≈ 0 且无异常。
10. **分桶校验**：100 万 uid 的 10 个实验分布与配置占比偏差 < 0.5%，正交交叉表符合期望。
