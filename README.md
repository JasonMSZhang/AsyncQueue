# AsyncQueue

基于 Java 17 + Spring Boot 的**自研异步任务队列**，不引入任何 MQ 中间件。以「AB-Test 分桶计算」为业务场景，演示任务持久化、有界优先级队列、背压、重试与死信、优雅停机、缓存与可观测指标的完整落地。

## 核心特性

- **不丢任务**：先把任务写入 H2 并提交事务，再尝试放入内存队列；进程崩溃后可从数据库恢复
- **自研有界优先级队列**：`ReentrantLock + Condition` + 有序数组，未使用 `PriorityBlockingQueue`
- **快速失败背压**：队列满抛 `TaskQueueFullException`，接口返回 429，任务已持久化不会丢
- **幂等提交**：`businessKey = md5(uid + 排序后的实验ID)`，未完成任务直接复用已有 taskId
- **异常分类与重试**：业务可重试 / 系统可恢复 / 不可恢复，指数退避，重试耗尽进死信
- **重试不冲击队列**：重试只回写数据库（`nextExecuteTime`），不回内存队列
- **优雅停机**：停接入 → 队列回写 → 等待在途任务 → 超时强制中断并标记失败
- **本地缓存**：Caffeine，key=`uid:expId`，TTL 180 秒
- **可观测**：Micrometer + Actuator，暴露 `/actuator/prometheus`

## 场景与任务模型

**场景**：AB-Test 分桶计算。客户端提交「用户 + 实验列表」，服务端异步计算每个实验的桶号与分组，结果可查。

| 项 | 说明 |
| --- | --- |
| 任务类型 | CPU 计算 + 本地缓存 + 模拟 IO 的混合任务 |
| 输入 | `uid`、`expIdList`（test1~test10）、`priority`、`maxRetry` |
| 输出 | 每个实验的 `bucket`（0~9999）与 `treatment` / `control` |
| 耗时特征 | MurmurHash3 为微秒级；Caffeine 为内存访问；默认 `Thread.sleep(50ms)` 模拟 IO；结果落库约 5~20ms |
| 失败概率 | 正常输入不失败；通过 uid 前缀**确定性**注入故障（`biz-retry*` 业务可重试、`sys-db*` 系统可恢复、`fatal*` 不可恢复），保证可复现；生产环境可替换为按比例随机注入 |
| 并发模型 | 等待占比高，按 `N = CPU × (1 + W/C)` 取 16 个消费线程；H2 单写者是真实瓶颈，线程再多只会加剧写锁竞争 |
| 接收成功边界 | 返回 200/202 代表 H2 事务已提交；返回 429（队列满）时任务同样已提交；只有 400/503 代表未接收 |
| 快速返回 | 提交接口只做「参数校验 + 一次 INSERT + 尝试入队」，不做任何业务计算，耗时在毫秒级 |

## 技术栈

| 组件 | 版本 / 说明 |
| --- | --- |
| Java | 17 |
| Spring Boot | 3.3.5 |
| 持久化 | Spring Data JPA + H2（file 模式） |
| 本地缓存 | Caffeine 3.1.x |
| 指标 | Micrometer + Prometheus Registry |
| 测试 | JUnit 5（H2 内存模式） |

## 目录结构

```
.
├── DESIGN.md                     # 设计文档
├── ai-chat-log.md                # 开发对话记录
├── pom.xml
└── src
    ├── main
    │   ├── java/com/example/asyncqueue
    │   │   ├── AsyncQueueApplication.java
    │   │   ├── controller/       # REST 接口 + 全局异常处理
    │   │   ├── entity/           # 任务表、结果表实体
    │   │   ├── enums/            # TaskStatus
    │   │   ├── exception/        # 业务/系统/不可恢复/停机异常
    │   │   ├── executor/         # TaskExecutor 消费线程池
    │   │   ├── lifecycle/        # 提交开关、优雅停机编排
    │   │   ├── metrics/          # Micrometer Gauge
    │   │   ├── queue/            # 自研有界优先级队列
    │   │   ├── repository/       # JPA Repository
    │   │   ├── scheduler/        # 重试 / 补偿扫描器
    │   │   ├── service/          # 提交、状态流转、AB 分桶
    │   │   └── util/             # MurmurHash3、businessKey
    │   └── resources
    │       ├── application.yml
    │       ├── schema-h2.sql
    │       └── static/index.html # 根路径网页表单
    └── test/java/com/example/asyncqueue
        ├── integration/          # 提交 / 执行流程集成测试
        ├── queue/                # 队列单元与并发测试
        └── service/              # AB 分桶与缓存测试
```

## 启动步骤

### 1. 环境要求

- JDK 17
- Maven 3.8+

```bash
java -version
mvn -v
```

### 2. 启动应用

```bash
# 方式一：开发模式
mvn spring-boot:run

# 方式二：打包后运行
mvn clean package -DskipTests
java -jar target/async-queue-0.0.1-SNAPSHOT.jar
```

启动时会在当前目录创建 H2 数据文件：

```
./data/abtask.mv.db
```

如需清空数据，停止应用后删除 `data/` 目录即可。

### 3. 运行测试

```bash
mvn test
```

测试使用 H2 内存库，不会污染 `data/` 下的文件。

## 网页访问地址

启动后默认端口 `8080`：

| 地址 | 说明 |
| --- | --- |
| http://localhost:8080/ | 任务提交与查询页面（两个表单） |
| http://localhost:8080/h2-console | H2 控制台（JDBC URL：`jdbc:h2:file:./data/abtask`，用户 `sa`，密码空） |
| http://localhost:8080/actuator/health | 健康检查 |
| http://localhost:8080/actuator/prometheus | Prometheus 指标抓取端点 |

## 接口说明与 curl 示例

### 提交任务

```bash
curl -X POST http://localhost:8080/task/submit \
  -d "uid=u10086" \
  -d "expIdList=test1" \
  -d "expIdList=test10" \
  -d "priority=5" \
  -d "maxRetry=3"
```

响应：

```json
{"taskId":"7a171979def0437a9cee1b09afbaa09c"}
```

参数说明：

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `uid` | 是 | 用户标识，作为 hash 输入的一部分 |
| `expIdList` | 是 | 实验ID，可重复传多个，取值 `test1` ~ `test10` |
| `priority` | 否 | 0~9，数值越大越先执行，默认 5 |
| `maxRetry` | 否 | 最大重试次数，默认 3 |

业务键由服务端生成：`md5("u:" + uid + ":" + 排序后的实验ID列表)`。

幂等规则（明确约定）：

- 同一 businessKey 存在 `PENDING` / `PROCESSING` 任务时，直接返回已有 taskId，不新建；
- 任务已进入终态（`SUCCESS` / `FAILED` / `DEAD_LETTER`）后，再次提交视为一次**新的计算请求**，生成新的 taskId。

### 查询任务详情

```bash
curl http://localhost:8080/task/7a171979def0437a9cee1b09afbaa09c
```

成功时的响应：

```json
{
  "taskId": "7a171979def0437a9cee1b09afbaa09c",
  "status": "SUCCESS",
  "retryCount": 0,
  "maxRetry": 3,
  "failReason": null,
  "createTime": "2026-11-22T22:19:57.07725",
  "updateTime": "2026-11-22T22:19:57.200592",
  "result": [
    {"expId": "test1",  "bucket": 7897, "treatment": false},
    {"expId": "test10", "bucket": 4871, "treatment": true}
  ]
}
```

- `status`：`PENDING` / `PROCESSING` / `SUCCESS` / `FAILED` / `DEAD_LETTER`
- `result` 仅在 `SUCCESS` 时返回；`treatment=true` 表示实验组，`false` 表示对照组
- 任务不存在返回 `404`

### 状态统计

```bash
curl http://localhost:8080/task/stat
```

```json
{"PENDING":1,"PROCESSING":0,"SUCCESS":2,"FAILED":0,"DEAD_LETTER":1}
```

### 错误码

| HTTP | 场景 | 响应示例 |
| --- | --- | --- |
| 400 | 参数缺失或实验ID非法 | `{"message":"不支持的实验ID: test999"}` |
| 404 | taskId 不存在 | 默认错误体 |
| 429 | 内存队列已满（任务已落库） | `{"message":"任务队列繁忙，任务已持久化，请稍后使用相同参数重试"}` |
| 503 | 服务正在停机 | `{"message":"服务正在停机，暂不接收新任务"}` |

> 429 时任务已经持久化，用**相同参数**重试会命中幂等，直接拿回同一个 taskId。

## 故障模拟（验证重试与死信）

为便于测试异常分支，`uid` 前缀会触发预设故障：

| uid 前缀 | 行为 |
| --- | --- |
| `biz-retry...` | 抛出业务可重试异常，前两次失败后成功；未达上限时回到 `PENDING` |
| `sys-db...` | 抛出系统可恢复异常（模拟数据库连接异常），第一次失败后成功 |
| `fatal...` | 抛出不可恢复异常，直接进入 `DEAD_LETTER` |

示例：提交一个必然进入死信的任务（`maxRetry=0`）

```bash
curl -X POST http://localhost:8080/task/submit \
  -d "uid=biz-retry-demo" -d "expIdList=test1" -d "maxRetry=0"
```

## 配置说明

`src/main/resources/application.yml` 中的关键配置：

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `async-queue.queue.capacity` | 4096 | 内存队列容量，满则触发背压 |
| `async-queue.executor.worker-count` | 16 | 消费线程数（8 核，CPU+IO 混合） |
| `async-queue.executor.simulate-io-millis` | 50 | 模拟业务 IO 耗时，0 表示不模拟 |
| `async-queue.executor.shutdown-timeout-seconds` | 20 | 停机等待在途任务的超时 |
| `async-queue.retry.base-delay-seconds` | 1 | 指数退避基础间隔 |
| `async-queue.retry.max-delay-seconds` | 60 | 退避上限 |
| `async-queue.scheduler.recovery-interval-millis` | 1000 | 重试/补偿扫描周期 |
| `async-queue.scheduler.recovery-batch-size` | 200 | 单轮最多投递任务数 |
| `async-queue.cache.ttl-seconds` | 180 | AB 分桶缓存 TTL |
| `async-queue.cache.max-size` | 200000 | AB 分桶缓存最大条目 |
| `spring.datasource.url` | `jdbc:h2:file:./data/abtask;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;WRITE_DELAY=0` | H2 文件库，提交即落盘 |

> `DB_CLOSE_ON_EXIT=FALSE` 必须保留。否则 JVM 收到 SIGTERM 时，H2 自身的 shutdown hook 会与 Spring 优雅停机并发执行，可能先关闭数据库，导致队列回写和在途任务状态回写失败。
>
> `WRITE_DELAY=0` 关闭 H2 默认的约 500ms 异步刷盘窗口，保证「事务提交成功即落盘」，代价是写入吞吐下降。

## 可观测指标

| 指标 | 类型 | 说明 |
| --- | --- | --- |
| `async_queue_size` | Gauge | 队列当前任务数 |
| `async_queue_capacity` | Gauge | 队列容量 |
| `async_executor_active_threads` | Gauge | 正在执行任务的消费线程数 |
| `async_task_submit_total{result}` | Counter | 提交成功 / 失败次数 |
| `async_task_exec_total{result}` | Counter | 任务执行结果：success / retry / dead_letter / failed |
| `async_task_retry_total` | Counter | 安排重试的总次数 |
| `async_task_queue_wait_seconds` | Timer | 入队到开始处理的排队时长 |
| `async_task_queue_wait_avg` | Gauge | 平均排队时长（毫秒） |
| `async_task_process_seconds` | Timer | 任务处理时长（含持久化） |
| `async_task_process_avg` | Gauge | 平均处理时长（毫秒） |

查看：

```bash
curl http://localhost:8080/actuator/prometheus | grep async_
```

Prometheus 抓取配置示例：

```yaml
scrape_configs:
  - job_name: async-queue
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ["127.0.0.1:8080"]
```

## 部署到服务器

### 1. 单实例约束（重要）

H2 file 模式同一时刻只允许一个进程写同一个库文件，因此：

- **只能单实例部署**，不支持多副本同时运行；
- 滚动发布必须「先停旧实例，再起新实例」，不能让两个进程同时指向同一个 `data/`；
- 需要水平扩展时，应先把存储替换为外部数据库并引入分布式锁，再扩展消费实例。

### 2. 目录与构建

```bash
mvn clean package -DskipTests
```

服务器目录示例：

```
/opt/async-queue/
├── app.jar
└── data/            # H2 数据文件目录，需要可写
```

启动命令：

```bash
cd /opt/async-queue
java -Xms512m -Xmx1g -XX:+UseG1GC -jar app.jar --server.port=8080
```

建议用专用低权限用户运行，并保证 `data/` 目录对该用户可写。

### 3. systemd 示例

`/etc/systemd/system/async-queue.service`：

```ini
[Unit]
Description=AsyncQueue
After=network.target

[Service]
User=appuser
WorkingDirectory=/opt/async-queue
ExecStart=/usr/lib/jvm/jdk-17/bin/java -Xms512m -Xmx1g -XX:+UseG1GC -jar app.jar --server.port=8080
# 收到 SIGTERM 后由应用执行优雅停机
KillSignal=SIGTERM
# 必须大于停机预算（默认约 23s，生命周期上限 30s）
TimeoutStopSec=45
SuccessExitStatus=143
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now async-queue
sudo systemctl status async-queue
```

### 4. Nginx 反向代理（可选）

```nginx
server {
    listen 80;
    server_name queue.example.com;

    location / {
        proxy_pass         http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header   Host              $host;
        proxy_set_header   X-Real-IP         $remote_addr;
        proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
        # 停机时优雅关闭连接，避免请求被硬切
        proxy_read_timeout 60s;
    }
}
```

> 公网部署时不要把 `/h2-console` 暴露出去，建议关闭 `spring.h2.console.enabled` 或用 Nginx 限制来源 IP。

### 5. 发布与停机

- 发布：`systemctl stop async-queue` → 替换 `app.jar` → `systemctl start async-queue`
- 停机流程（SIGTERM 触发）：
  1. 关闭提交开关，新提交返回 503；
  2. `drainAll()` 把内存队列任务批量回写 `PENDING`；
  3. 关闭队列入口，唤醒空闲消费者；
  4. 等待在途任务完成（默认 20s），超时 `shutdownNow()` 并把被中断任务标记 `FAILED`。
- `kill -9` 不会走上述流程，任务也不会丢：
  - 内存队列里尚未执行的任务，在数据库中一直是 `PENDING`；
  - 正在执行的任务在数据库中是 `PROCESSING`；
  - 单实例重启时，启动流程会先把所有 `PROCESSING` 重置为 `PENDING`，再连同其余 `PENDING` 任务一起重新加载执行（AB 分桶是纯计算，重跑幂等且安全）。
- 因此**崩溃恢复的边界**是：内存队列会丢，但数据库不会丢；恢复的代价是「未完成任务可能被重复执行一次」。

### 6. 数据备份与磁盘

```bash
# 停机后直接备份数据文件
systemctl stop async-queue
cp -a /opt/async-queue/data/abtask.mv.db /backup/abtask-$(date +%F).mv.db
systemctl start async-queue
```

- H2 无副本，磁盘写满会直接导致不可用，建议监控磁盘水位（<20% 告警）
- 定期备份 `data/abtask.mv.db`；备份前请停止写入（停应用），避免文件不一致

### 7. 健康检查与监控

- 存活检查：`GET /actuator/health`
- 指标抓取：`GET /actuator/prometheus`
- 建议告警项：
  - `async_queue_size / async_queue_capacity > 80%`
  - `async_task_submit_total{result="failure"}` 增长
  - `async_task_retry_total` 突增
  - 死信数量（`status='DEAD_LETTER'`）新增
  - `async_task_queue_wait_seconds` 的 p99 持续升高

## 已知限制

1. **单实例约束**：受 H2 file 模式限制，无法多实例共享同一库；启动时把 `PROCESSING` 重置为 `PENDING` 的策略也以「同一时刻只有一个实例」为前提，误开双实例会互相抢任务。
2. **无运行时租约/心跳**：只在启动时回收 `PROCESSING`。如果进程没有退出、只是某个任务线程假死，该任务会长期停留在 `PROCESSING`，需要额外的租约机制。
3. **幂等存在并发窗口**：当前是「先查后插」，同一 businessKey 的高并发提交仍可能产生两条任务；彻底解决需要给主表增加活跃键唯一索引。
4. **重试依赖扫描周期**：重试任务由补偿扫描器按 `nextExecuteTime` 重新投递，最小延迟受扫描周期（默认 1s）影响。

## 相关文档

- [DESIGN.md](./DESIGN.md) — 完整设计文档（队列、背压、重试、停机、指标、AB 分桶）
- [ai-chat-log.md](./ai-chat-log.md) — 开发过程对话记录
