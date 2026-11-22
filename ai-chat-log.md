# AI 对话记录（AsyncQueue）

> 记录每一轮用户 Prompt 与 AI 返回的关键片段。仅保留决策、产出文件、验证结论，省略过程中的完整代码与冗长日志。

---

## 第 1 轮：输出设计文档

### Prompt（节选）

> 背景：Java17 SpringBoot 项目，实现自研异步任务队列，不引入 MQ 中间件；存储使用 H2 文件持久模式 spring-data-jpa。业务场景：AB-Test 分桶计算任务。
>
> 业务场景说明：
> 1. AB-Test 分桶任务：输入 userId/deviceId，多个实验 ID test1-test10；分桶总数量 10000，桶号 0-9999。实验组比例 5% / 15% / … / 95%，其余桶为对照组。
> 2. hash 规则：MurmurHash3，hash 原始字符串 = "{uid}:{expId}"，对 10000 取模得到桶号，保证不同实验正交隔离。
> 3. 使用 Caffeine 本地缓存，key=uid:expId，TTL=180 秒；命中直接返回；未命中计算并回填。
> 4. 任务特征：CPU 计算 + 本地缓存 IO 混合；模拟 sleep 制造耗时；模拟业务异常、数据库系统异常测试重试。
> 5. 业务幂等 businessKey：对 uid + 实验 id 列表做 md5；相同业务键未完成则返回已有 taskId。
>
> 请输出一份设计文档，不要写代码，包含：任务模型与状态枚举、两张表字段、持久化边界与 kill-9 取舍、内存有界优先级队列容量决策与背压选型、线程池参数、异常分类、重试策略与死信、优雅停机流程、可观测指标、AB-Test 实现要点。
>
> 硬性约束：
> 1. 基于 JDK Lock + Condition 实现有界优先级队列，禁止直接使用 PriorityBlockingQueue 作为实现主体；
> 2. 提交顺序：先写 H2 成功，再尝试放入内存队列；
> 3. 内存队列满触发背压；重试任务不直接塞回内存队列，持久化到数据库，项目启动加载 pending 任务。

### AI 返回（关键片段）

- 产出 `DESIGN.md`，591 行，包含 10 个章节。
- 核心结论：
  - 数据库是唯一事实来源，内存队列 / Caffeine / 在途任务都可丢弃重建。
  - 幂等用 `active_key`（非终态等于 businessKey、终态置 NULL + 唯一索引），避免给 business_key 建唯一索引导致终态后无法重提。
  - 指出 H2 默认 `WRITE_DELAY=500ms` 会破坏「返回即不丢」，严格模式需设为 0。
  - 队列容量 4096：峰值 300 tps × 5s ≈ 1500，处理能力 200/s 的 2 倍突发 ≈ 2400，内存上界 8192。
  - 背压三段传导：执行层拒绝 → Dispatcher 变慢 → 队列满 → 接口返回 deferred。
  - 重试只落库 + 独立低频扫描器，优先级降级、全抖动、重试流量预算 20%、按类型熔断。
  - kill -9 的代价转化为「可接受重复执行」，依赖先落库、幂等业务、租约回收三条前提。
  - 提醒 MurmurHash3 必须锁定实现与取位，取桶用 `floorMod`。

---

## 第 2 轮：提交初始化 commit

### Prompt

> 使用 github 插件帮我：提交一下当前的改动，备注：初始化提交

### AI 返回（关键片段）

- 检查发现仓库未配置 Git 身份，按上一个提交作者在**当前仓库**设置 `JasonMSZhang <2217841389@qq.com>`。
- 提交 `f7fbd5c 初始化提交`，新增 `DESIGN.md` 591 行，工作区干净。
- 提示：`main` 领先 origin 1 个提交，未推送。

---

## 第 3 轮：推送到 origin

### Prompt

> 好的，推送到 origin

### AI 返回（关键片段）

- `faa2f40..f7fbd5c main -> main`，`main` 与 `origin/main` 同步。

---

## 第 4 轮：生成 SpringBoot 工程基础代码

### Prompt（节选）

> 基于上一步的 design.md 生成 SpringBoot 工程基础代码，Java17，lombok，spring-boot-starter-data-jpa，h2，caffeine，micrometer。
> H2 文件模式 `jdbc:h2:file:./data/abtask;DB_CLOSE_DELAY=-1`；启动自动执行 schema-h2.sql。
> 1. TaskStatus 枚举：PENDING、PROCESSING、SUCCESS、FAILED、DEAD_LETTER。
> 2. TaskEntity 字段：taskId、businessKey、priority、taskParamJson、status、retryCount、maxRetry、nextExecuteTime、failReason、createTime、updateTime。
> 3. TaskResultEntity：id、taskId、resultJson、completeTime，与 task 一对一，两张表严格分离。
> 4. JpaRepository：按 businessKey 查询未结束任务、查询待执行 PENDING、按状态统计 count。
> 输出 pom.xml、application.yml、schema-h2.sql、实体类、Repository。禁止任何 MQ 中间件。

### AI 返回（关键片段）

- 新增 `pom.xml`（Boot 3.3.5 / Java 17）、`application.yml`、`schema-h2.sql`、`AsyncQueueApplication`、`.gitignore`。
- 实体与仓库：
  - `TaskStatus`：含 `isTerminal()` 与 `unfinished()`（PENDING + PROCESSING）。
  - `TaskEntity` / `TaskResultEntity`：结果表用普通 taskId + 唯一约束表达 1:1，不建 JPA 关联。
  - `TaskRepository`：`findFirstByBusinessKeyAndStatusInOrderByCreateTimeDesc`、按优先级查询待执行、`countByStatus`。
- 额外加入 `spring-boot-starter-web`：无 web 依赖时非 web 应用启动后会立即退出，且 `/h2-console` 需要容器。
- 验证：`Found 2 JPA repository interfaces.`，H2 中确认 `T_TASK`、`T_TASK_RESULT` 及索引均已创建。

---

## 第 5 轮：自研有界优先级队列

### Prompt（节选）

> 实现自研有界优先级内存队列，不使用消息中间件，底层使用 JDK ReentrantLock + Condition，禁止直接封装 PriorityBlockingQueue，内部自己维护有序任务列表。
> 1. 容量可配置、有界；队列满时 offer 抛自定义 TaskQueueFullException 作为背压。
> 2. 支持优先级，高优先级优先被 take。
> 3. API：`offer(TaskEntity) boolean`、`take()` 阻塞、`size()`、`getCapacity()`、`isFull()`、`drainAll()`。
> 4. 多生产者多消费者线程安全。输出完整 Java 类，增加关键注释。

### AI 返回（关键片段）

- 新增 `queue/BoundedPriorityTaskQueue.java`、`queue/TaskQueueFullException.java`。
- 实现：`ReentrantLock` + 内部 `ArrayList` + `Collections.binarySearch` 插入；排序 priority 降序 → createTime 升序 → taskId 兜底；队列满快速失败，因此只有 `notEmpty` 一个 Condition；`take()` 可中断。
- 容量通过 `@Value("${async-queue.queue.capacity:4096}")` 注入并写入 yml。
- 验证（临时程序）：5 场景通过，含 6 生产者 × 2000 任务 + 6 消费者，12000 条不丢不重。

---

## 第 6 轮：提交现有 commit

### Prompt

> 提交现有的 commit

### AI 返回（关键片段）

- 工作区干净，改动已由用户提交：`760be68`、`a9fbd1e`。
- 说明 `main` 领先 origin 1 个提交，询问是否推送。

---

## 第 7 轮：推送

### Prompt

> 推送吧

### AI 返回（关键片段）

- 直连 GitHub 失败（`HTTP2 framing layer` / 连接超时）。
- 改用本机 ClashX 代理 `127.0.0.1:7890` 后成功：`a9fbd1e..760be68 main -> main`，未改仓库 git 配置。

---

## 第 8 轮：TaskSubmitService

### Prompt（节选）

> 基于实体、Repository、自研内存队列，编写 TaskSubmitService。业务场景 AB-Test 分桶任务。
> 1. `submitTask(String businessKey, String paramJson, int priority, int maxRetry)` 返回 taskId。
> 2. 幂等：通过 businessKey 查询，若存在 PENDING/PROCESSING 任务，直接返回已有 taskId。
> 3. 关键顺序：先执行 H2 insert 成功，再尝试 offer 进入内存队列。
> 4. 队列满抛 TaskQueueFullException，此时数据库已保存任务。
> 5. 初始状态 PENDING。绝不允许先入内存再落库。

### AI 返回（关键片段）

- 新增 `service/TaskSubmitService.java`。
- 方法本身不加 `@Transactional`，落库走 `TransactionTemplate` + `PROPAGATION_REQUIRES_NEW`，确保 commit 发生在 offer 之前。
- 队列满只记录日志并原样抛出；客户端用相同 businessKey 重试会命中幂等拿回 taskId。
- 验证：持久化→入队顺序、同键复用不重复入队、队列满时任务已落库 PENDING、重试取回同一 taskId。
- 提示已知窗口：先查后插在并发同键下仍可能插入两条（schema 无 active_key）。

---

## 第 9 轮：TaskExecutor 消费组件

### Prompt（节选）

> 编写 Spring Bean TaskExecutor 任务消费组件。
> 任务参数 json 含 uid、expId 列表；MurmurHash3 分桶（uid:expId）；Caffeine TTL 180s；Thread.sleep 模拟 IO；模拟两类异常（业务可重试、系统可恢复、不可恢复进死信）。
> 1. 线程池配置放 application 配置文件，适配 CPU+IO 混合；启动 N 个线程从自研队列 take()。
> 2. 取到任务后更新 DB 为 PROCESSING。
> 3. 执行分桶，成功写 TaskResultEntity，状态置 SUCCESS。
> 4. 可重试异常：未达 maxRetry 指数退避写 nextExecuteTime、回 PENDING，**不重新 offer**；超限 DEAD_LETTER；不可恢复直接死信。
> 5. 启动时从 H2 加载 PENDING 任务进内存队列。
> 6. 重试任务只存 DB，不回队列。

### AI 返回（关键片段）

- 新增：`executor/TaskExecutor.java`、`service/TaskStateService.java`、`service/AbTestBucketService.java`、`util/MurmurHash3.java`、三个异常类。
- `TaskRepository` 增加状态 CAS 更新与 taskId 游标分页（避免 offset 分页漏读）。
- 线程数 16（8 核，N = CPU × (1 + W/C)），启动先拉起消费者再加载 PENDING。
- 修掉一个真实启动问题：类名 `TaskExecutor` 的默认 bean 名与 Spring Boot `applicationTaskExecutor` 的别名冲突，改为 `@Component("abTaskExecutor")`。
- 验证：启动加载执行成功、正常任务 SUCCESS、可重试任务 PENDING/retryCount=1 且未自动重跑、fatal 直接 DEAD_LETTER。
- 明确指出缺口：进程内没有扫描器，重试任务只有重启才会执行；也没有租约回收。

---

## 第 10 轮：优雅停机

### Prompt（节选）

> 实现 Spring 优雅停机逻辑，流程严格执行：
> 1. 设置全局开关，停止接收新任务提交；
> 2. 调用 drainAll() 取出全部内存未消费任务，批量更新 H2 置 PENDING，打印回写数量；
> 3. 关闭工作线程池，awaitTermination 超时后 shutdownNow；
> 4. 正在运行的任务尽量执行完成，超时标记失败记录原因。

### AI 返回（关键片段）

- 新增 `lifecycle/TaskSubmissionGate.java`、`lifecycle/GracefulShutdownManager.java`、`exception/TaskSubmitClosedException.java`。
- 队列增加 `close()`，关闭后 `take()` 返回 null；`offer` 拒绝。
- `TaskStateService` 增加 `resetToPending`（500 条一批）与 `markFailed`（停机中断置 FAILED）。
- `TaskExecutor.shutdown(timeout)`：`shutdown()` → `awaitTermination` → 超时 `shutdownNow()`。
- `getPhase()` 返回 `Integer.MAX_VALUE`，保证回写发生在 JPA/DataSource 关闭之前。
- 验证场景 A（5s 超时/800ms 任务）：`SUCCESS=1, PENDING=2`，停机后提交被 503 拒绝；场景 B（1s 超时/5s 任务）：`FAILED/优雅停机超时，任务被强制中断`。

---

## 第 11 轮：推送已提交内容

### Prompt

> 把已经提交的内容 push 上去

### AI 返回（关键片段）

- `2366277..5af3e8c main -> main`，仍通过本机 7890 代理，未改全局配置。

---

## 第 12 轮：Controller + HTML 页面 + Micrometer

### Prompt（节选）

> 编写 SpringMVC Controller，以及一个简单 html 页面，访问根路径 `/` 打开网页表单。
> 1. `POST /task/submit` 接收 uid、expIdList 数组、优先级、最大重试；内部生成 businessKey；返回 taskId。
> 2. `GET /task/{taskId}` 查询状态、已重试次数、failReason；成功返回分桶结果。
> 3. `GET /task/stat` 各状态数量。
> 页面：表单①提交并展示 taskId；表单②按 taskId 查询状态与各实验分组结果。
> 指标：队列 size、容量、活跃线程数、提交成功/失败计数、重试总计数、平均排队时长、平均处理时长；开启 actuator/prometheus。

### AI 返回（关键片段）

- 新增 `controller/TaskController.java`、`controller/ApiExceptionHandler.java`、`static/index.html`、`metrics/AsyncQueueMetrics.java`、`util/BusinessKeyUtils.java`。
- 异常映射：参数错误 400、队列满 429、停机 503、任务不存在 404。
- pom 增加 actuator + micrometer-registry-prometheus，yml 暴露 `health,info,metrics,prometheus`。
- 指标实现要点：`active.threads` 不用 `ThreadPoolExecutor.getActiveCount()`（worker 长循环恒等于线程数），改为在任务执行期间累加/递减。
- HTTP 验证通过：首页 200、提交/查询、test1/test10 分组判定、死信、重试、统计、404、Prometheus 指标齐全。
- 实测：`async_executor_active_threads` 运行中 1.0、结束后 0.0；`async_task_retry_total 1.0`；`async_task_submit_total{result="success"} 4.0`。

---

## 第 13 轮：JUnit 5 单元与并发测试

### Prompt（节选）

> 编写 JUnit5 单元&并发测试；测试环境 H2 内存模式。
> 1. 幂等提交：相同 businessKey 返回同一 taskId。
> 2. 队列满，背压抛 TaskQueueFullException。
> 3. AB 分桶：同 uid+expId 稳定；不同实验正交；Caffeine 命中逻辑。
> 4. 任务成功流程；失败重试，达到上限进死信。
> 5. 并发多线程提交测试。

### AI 返回（关键片段）

- 新增 4 个测试类，共 17 个用例：
  - `BoundedPriorityTaskQueueTest`（6）：优先级顺序、同优先级 FIFO、队满、drainAll、close、并发不丢不重。
  - `AbTestBucketServiceTest`（4）：结果稳定、2000 样本比例与正交性、缓存 miss→hit、未知实验异常。
  - `TaskSubmitIntegrationTest`（3）：幂等、背压且已落库、10 线程并发提交。
  - `TaskExecutionIntegrationTest`（4）：成功、可重试回 PENDING、重试耗尽死信、不可恢复死信。
- 集成测试用 `@SpringBootTest(webEnvironment=NONE)` + `jdbc:h2:mem:*`。
- 给 `AbTestBucketService` 增加只读 `cacheStats()` 以断言命中。
- 过程中修正测试自身问题：businessKey 前缀超长（44 字符）超出 `VARCHAR(32)`，改为 32 位 UUID。
- 结果：`Tests run: 17, Failures: 0, Errors: 0, Skipped: 0`，BUILD SUCCESS。

---

## 第 14 轮：生成本对话记录

### Prompt

> 新建 `ai-chat-log.md` 文件，把每一轮 prompt 和 AI 返回关键片段写入到这个文件中

### AI 返回（关键片段）

- 新增 `ai-chat-log.md`（本文件），按轮次记录 Prompt 与关键产出、决策、验证结论。
- Prompt 过长处做节选，保留需求要点与硬性约束；AI 返回只保留文件产出、关键决策与验证结果。

---

## 第 15 轮：功能清单全场景验证

### Prompt

> 验证全部功能清单（跑通全部场景）
> 1. 提交任务返回 taskId；相同 businessKey 幂等返回同一个 taskId；
> 2. 内存队列打满，触发背压抛出异常；
> 3. 任务正常流转 PENDING → PROCESSING → SUCCESS，可以查询结果；
> 4. 模拟业务失败，自动重试，超过最大重试进入死信；
> 5. JVM 优雅停机：内存排队任务回写数据库；重启，任务重新加载继续执行；
> 6. JVM 直接 kill -9 模拟崩溃：内存队列未处理任务内存丢失但 DB 存在；重启后重新加载执行；该边界要写进文档；
> 7. 访问 actuator 查看监控指标；状态统计接口返回正确数字。

### AI 返回（关键片段）

补齐两处验证必需能力：

1. 新增 `scheduler/TaskRecoveryScheduler`：每秒扫描到期的 `PENDING` 任务（重试任务 + 背压 deferred 任务）重新投递；
   队列增加 `contains(taskId)` 去重，避免重复占位。
2. 新增启动恢复：`TaskStateService.recoverProcessingTasks()`，把上次异常退出遗留的 `PROCESSING` 重置为 `PENDING`（单实例前提）。

**发现并修复一个真实停机缺陷**：首次 SIGTERM 验证时出现
`Database is already closed (to disable automatic closing at VM shutdown, add ";DB_CLOSE_ON_EXIT=FALSE")`。
原因是 H2 自身的 shutdown hook 与 Spring 优雅停机并发执行，提前关闭数据库，导致队列回写与在途任务状态回写失败。
修复：JDBC URL 增加 `DB_CLOSE_ON_EXIT=FALSE`；同时让回写失败不再中断后续停机步骤。

验证结果（真实进程 + HTTP + H2 文件）：

| 场景 | 结果 |
| --- | --- |
| 1 幂等 | 两次提交返回同一 taskId（`6fcb7cbf...`） |
| 2 背压 | 第三个提交返回 429，响应说明任务已持久化 |
| 3 正常流转 | `SUCCESS`，返回 2 条分桶结果，分组与桶号一致 |
| 4 自动重试 | 首次失败 → PENDING/retryCount=1 → 扫描器重新投递 → 最终 SUCCESS，retryCount=2 |
| 4 重试耗尽 | maxRetry=1 → `DEAD_LETTER`，原因「重试次数已耗尽(已重试1次)」 |
| 4 不可恢复 | fatal uid → 直接 `DEAD_LETTER`，retryCount=0 |
| 5 优雅停机 | `drained=1, updated=1`；在途任务完成；DB `SUCCESS=1, PENDING=2` |
| 5 重启恢复 | 重启日志 `loaded=2`，最终 `SUCCESS=3` |
| 6 kill -9 | 崩溃后 DB `PROCESSING=1, PENDING=2`；重启日志「启动恢复: 1 个 PROCESSING 任务重置为 PENDING」「loaded=3」，最终 `SUCCESS=3` |
| 7 统计/指标 | `/task/stat` 数字正确；`/actuator/prometheus` 指标齐全，`async_task_retry_total=5.0` |

边界结论（写进 README / DESIGN）：

> kill -9 时内存队列会丢，但数据库不会丢：排队任务在库中一直是 `PENDING`，在途任务是 `PROCESSING`。
> 单实例重启时先把 `PROCESSING` 重置为 `PENDING`，再统一加载执行；代价是「未完成任务可能被重复执行一次」，因此要求任务幂等。

---

## 附：最终代码结构

```
src/main/java/com/example/asyncqueue/
├── AsyncQueueApplication.java
├── controller/        TaskController, ApiExceptionHandler
├── entity/            TaskEntity, TaskResultEntity
├── enums/             TaskStatus
├── exception/         BusinessRetryableException, SystemRecoverableException,
│                      UnrecoverableTaskException, TaskSubmitClosedException
├── executor/          TaskExecutor
├── lifecycle/         TaskSubmissionGate, GracefulShutdownManager
├── metrics/           AsyncQueueMetrics
├── queue/             BoundedPriorityTaskQueue, TaskQueueFullException
├── repository/        TaskRepository, TaskResultRepository
├── scheduler/         TaskRecoveryScheduler
├── service/           TaskSubmitService, TaskStateService, AbTestBucketService
└── util/              MurmurHash3, BusinessKeyUtils

src/main/resources/
├── application.yml
├── schema-h2.sql
└── static/index.html

src/test/java/com/example/asyncqueue/
├── integration/       TaskSubmitIntegrationTest, TaskExecutionIntegrationTest
├── queue/             BoundedPriorityTaskQueueTest
└── service/           AbTestBucketServiceTest
```

## 附：关键参数

| 参数 | 值 |
| --- | --- |
| 队列容量 | 4096 |
| 消费线程数 | 16（8 核，CPU+IO 混合） |
| 分桶总数 | 10000 |
| 缓存 TTL / 上限 | 180s / 200000 |
| 重试退避 | base 1s，×2，上限 60s |
| 停机预算 | 停机流程 20s + 3s，生命周期上限 30s |
| H2 URL | `jdbc:h2:file:./data/abtask;DB_CLOSE_DELAY=-1` |
