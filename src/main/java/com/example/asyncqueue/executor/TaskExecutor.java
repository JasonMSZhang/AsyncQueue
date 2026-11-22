package com.example.asyncqueue.executor;

import com.example.asyncqueue.entity.TaskEntity;
import com.example.asyncqueue.enums.TaskStatus;
import com.example.asyncqueue.exception.BusinessRetryableException;
import com.example.asyncqueue.exception.SystemRecoverableException;
import com.example.asyncqueue.exception.UnrecoverableTaskException;
import com.example.asyncqueue.queue.BoundedPriorityTaskQueue;
import com.example.asyncqueue.queue.TaskQueueFullException;
import com.example.asyncqueue.repository.TaskRepository;
import com.example.asyncqueue.service.AbTestBucketService;
import com.example.asyncqueue.service.TaskStateService;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 任务消费组件。
 *
 * <p>职责：</p>
 * <ol>
 *   <li>启动 N 个消费线程，循环从自研有界优先级队列 {@code take()} 任务；</li>
 *   <li>领取后先把任务状态 CAS 成 PROCESSING，避免重复消费；</li>
 *   <li>执行 AB-Test 分桶（MurmurHash3 + Caffeine 180s 缓存），结果入库并把任务置 SUCCESS；</li>
 *   <li>按异常类型分流：可重试 -> 指数退避回写 PENDING（只落库，<b>不回内存队列</b>）；
 *       不可恢复/重试耗尽 -> DEAD_LETTER；</li>
 *   <li>应用启动时把数据库中到期的 PENDING 任务加载回内存队列。</li>
 * </ol>
 */
@Slf4j
@Component("abTaskExecutor")
@Order(0)
public class TaskExecutor implements ApplicationRunner {

    /** 启动加载的单批大小 */
    private static final int STARTUP_LOAD_BATCH = 500;

    /** 启动加载遇到队满时的重试次数与间隔 */
    private static final int OFFER_RETRY_TIMES = 3;
    private static final long OFFER_RETRY_INTERVAL_MILLIS = 100L;

    private final BoundedPriorityTaskQueue taskQueue;
    private final TaskRepository taskRepository;
    private final TaskStateService taskStateService;
    private final AbTestBucketService abTestBucketService;
    private final ObjectMapper objectMapper;

    private final int workerCount;
    private final long simulateIoMillis;
    private final long shutdownTimeoutSeconds;

    /** 排队时长 / 处理时长指标 */
    private final Timer queueWaitTimer;
    private final Timer processTimer;

    /** 消费线程池；worker 是长循环，任务数固定等于线程数 */
    private ThreadPoolExecutor workerPool;

    /** 运行标记，停机时置 false，让 take() 返回后不再继续循环 */
    private volatile boolean running;

    /** 停机幂等标记，避免 SmartLifecycle 与 @PreDestroy 重复关闭 */
    private final AtomicBoolean shutdownStarted = new AtomicBoolean(false);

    /** 当前正在执行任务的线程数（线程池 getActiveCount 会因为 worker 长循环恒等于线程总数，不可用） */
    private final AtomicInteger busyWorkers = new AtomicInteger();

    public TaskExecutor(BoundedPriorityTaskQueue taskQueue,
                        TaskRepository taskRepository,
                        TaskStateService taskStateService,
                        AbTestBucketService abTestBucketService,
                        ObjectMapper objectMapper,
                        MeterRegistry meterRegistry,
                        @Value("${async-queue.executor.worker-count:16}") int workerCount,
                        @Value("${async-queue.executor.simulate-io-millis:50}") long simulateIoMillis,
                        @Value("${async-queue.executor.shutdown-timeout-seconds:20}") long shutdownTimeoutSeconds) {
        if (workerCount <= 0) {
            throw new IllegalArgumentException("消费线程数必须大于 0, 当前值: " + workerCount);
        }
        this.taskQueue = taskQueue;
        this.taskRepository = taskRepository;
        this.taskStateService = taskStateService;
        this.abTestBucketService = abTestBucketService;
        this.objectMapper = objectMapper;
        this.workerCount = workerCount;
        this.simulateIoMillis = simulateIoMillis;
        this.shutdownTimeoutSeconds = shutdownTimeoutSeconds;
        this.queueWaitTimer = Timer.builder("async.task.queue.wait")
                .description("任务从入队到开始处理的排队时长")
                .register(meterRegistry);
        this.processTimer = Timer.builder("async.task.process")
                .description("任务处理时长（含持久化）")
                .register(meterRegistry);

        // 需求要求直接暴露平均排队/处理时长：由 Timer 的 sum/count 计算，毫秒
        Gauge.builder("async.task.queue.wait.avg", queueWaitTimer,
                        timer -> timer.mean(TimeUnit.MILLISECONDS))
                .description("平均排队时长（毫秒）")
                .register(meterRegistry);
        Gauge.builder("async.task.process.avg", processTimer,
                        timer -> timer.mean(TimeUnit.MILLISECONDS))
                .description("平均处理时长（毫秒）")
                .register(meterRegistry);
    }

    /**
     * 启动流程：先拉起消费线程，再把数据库中到期的 PENDING 任务加载进队列。
     *
     * <p>先启动消费者再加载，是为了让加载过程中队列能持续被消费，降低「一启动就队满」的概率。</p>
     */
    @Override
    public void run(ApplicationArguments args) {
        // 单实例前提：上次异常退出遗留的 PROCESSING 任务没有执行者，先回收为 PENDING
        taskStateService.recoverProcessingTasks();
        startWorkers();
        loadPendingTasks();
    }

    private void startWorkers() {
        running = true;
        // 固定大小线程池：core == max，队列只用于提交这 N 个长循环任务
        workerPool = new ThreadPoolExecutor(
                workerCount,
                workerCount,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new WorkerThreadFactory());
        workerPool.prestartAllCoreThreads();
        for (int i = 0; i < workerCount; i++) {
            workerPool.execute(this::workerLoop);
        }
        log.info("任务消费线程池已启动: workerCount={}, simulateIoMillis={}", workerCount, simulateIoMillis);
    }

    /**
     * 消费线程主循环：阻塞 take，取到任务后处理。
     * 任何单任务异常都不允许让循环退出。
     */
    private void workerLoop() {
        String workerName = Thread.currentThread().getName();
        log.info("消费线程启动: {}", workerName);
        while (running) {
            TaskEntity task;
            try {
                task = taskQueue.take();
            } catch (InterruptedException e) {
                // 停机信号
                Thread.currentThread().interrupt();
                break;
            }
            if (task == null) {
                // 队列已关闭且为空，停机时正常退出
                break;
            }
            try {
                process(task);
            } catch (Throwable t) {
                log.error("任务处理出现未捕获异常，消费循环继续: taskId={}", task.getTaskId(), t);
            }
        }
        log.info("消费线程退出: {}", workerName);
    }

    private void process(TaskEntity task) {
        String taskId = task.getTaskId();

        // 排队时长：入队时间由提交方/启动加载写入（transient 字段）
        Long enqueueTimeMillis = task.getEnqueueTimeMillis();
        if (enqueueTimeMillis != null) {
            queueWaitTimer.record(Duration.ofMillis(Math.max(0, System.currentTimeMillis() - enqueueTimeMillis)));
        }

        // 1. CAS 领取：只有 PENDING 才能变成 PROCESSING，避免重复执行
        if (!taskStateService.markProcessing(taskId)) {
            log.debug("任务状态已变更，跳过本次执行: taskId={}", taskId);
            return;
        }

        long startMillis = System.currentTimeMillis();
        busyWorkers.incrementAndGet();
        try {
            // 2. 执行 AB-Test 分桶业务
            String resultJson = executeBusiness(task);

            // 3. 结果入库 + 状态置 SUCCESS（同一事务）
            taskStateService.markSuccess(taskId, resultJson);
            log.info("任务执行完成: taskId={}, cost={}ms", taskId, System.currentTimeMillis() - startMillis);

        } catch (BusinessRetryableException | SystemRecoverableException e) {
            // 业务可重试 / 系统可恢复：只回写数据库，不重新入队
            log.warn("任务可重试失败: taskId={}, type={}, reason={}",
                    taskId, e.getClass().getSimpleName(), e.getMessage());
            taskStateService.handleFailure(taskId, e.getMessage(), true);

        } catch (UnrecoverableTaskException e) {
            // 不可恢复：直接死信
            log.error("任务不可恢复，进入死信: taskId={}, reason={}", taskId, e.getMessage());
            taskStateService.handleFailure(taskId, e.getMessage(), false);

        } catch (InterruptedException e) {
            // 停机超时被强制中断：按需求标记失败并记录原因（不再自动重试）
            log.warn("任务执行被强制中断，标记失败: taskId={}", taskId);
            try {
                taskStateService.markFailed(taskId, "优雅停机超时，任务被强制中断");
            } catch (Exception ex) {
                log.error("中断后标记失败时出现异常: taskId={}", taskId, ex);
            } finally {
                Thread.currentThread().interrupt();
            }

        } catch (Exception e) {
            // 未知异常按系统可恢复处理：可能是一次瞬时故障
            log.error("任务执行出现未知异常，按系统可恢复处理: taskId={}", taskId, e);
            taskStateService.handleFailure(taskId, "未知异常: " + e.getMessage(), true);

        } finally {
            // 4. 处理时长（含持久化），无论成功失败都记录
            busyWorkers.decrementAndGet();
            processTimer.record(Duration.ofMillis(System.currentTimeMillis() - startMillis));
        }
    }

    /**
     * 当前正在执行任务的消费线程数，供 Micrometer Gauge 使用。
     */
    public int getBusyThreadCount() {
        return busyWorkers.get();
    }

    /**
     * AB-Test 分桶业务逻辑。
     *
     * @return 结果 JSON（每个实验一条：expId / bucket / treatment）
     */
    private String executeBusiness(TaskEntity task) throws InterruptedException {
        AbTaskParam param = parseParam(task.getTaskParamJson());

        // 模拟故障：按 uid 前缀触发，便于验证重试与死信逻辑
        simulateFailure(param.uid(), task.getRetryCount());

        // 模拟业务 IO 耗时
        if (simulateIoMillis > 0) {
            Thread.sleep(simulateIoMillis);
        }

        List<AbTestBucketService.BucketResult> results = new ArrayList<>(param.expIds().size());
        for (String expId : param.expIds()) {
            // 命中缓存直接返回，未命中计算并写回缓存
            results.add(abTestBucketService.bucketOf(param.uid(), expId));
        }

        try {
            return objectMapper.writeValueAsString(results);
        } catch (JsonProcessingException e) {
            throw new UnrecoverableTaskException("分桶结果序列化失败", e);
        }
    }

    /**
     * 解析任务参数。JSON 结构错误、uid 缺失、实验列表为空都属于不可恢复：相同输入重试必然失败。
     */
    private AbTaskParam parseParam(String paramJson) {
        AbTaskParam param;
        try {
            param = objectMapper.readValue(paramJson, AbTaskParam.class);
        } catch (JsonProcessingException e) {
            throw new UnrecoverableTaskException("任务参数 JSON 解析失败", e);
        }
        if (param.uid() == null || param.uid().isBlank()) {
            throw new UnrecoverableTaskException("任务参数缺少 uid");
        }
        if (param.expIds() == null || param.expIds().isEmpty()) {
            throw new UnrecoverableTaskException("任务参数缺少 expIds");
        }
        List<String> normalized = new ArrayList<>(param.expIds().size());
        for (String expId : param.expIds()) {
            if (expId == null || expId.isBlank()) {
                throw new UnrecoverableTaskException("任务参数包含空实验ID");
            }
            normalized.add(expId.trim());
        }
        return new AbTaskParam(param.uid().trim(), normalized);
    }

    /**
     * 故障模拟（仅用于验证测试）：
     * <ul>
     *   <li>uid 以 biz-retry 开头且 retryCount &lt; 2：抛业务可重试异常；</li>
     *   <li>uid 以 sys-db 开头且 retryCount &lt; 1：抛系统可恢复异常（模拟数据库连接异常）；</li>
     *   <li>uid 以 fatal 开头：抛不可恢复异常。</li>
     * </ul>
     */
    private void simulateFailure(String uid, int retryCount) {
        if (uid.startsWith("biz-retry") && retryCount < 2) {
            throw new BusinessRetryableException("模拟业务临时性异常, uid=" + uid);
        }
        if (uid.startsWith("sys-db") && retryCount < 1) {
            throw new SystemRecoverableException("模拟数据库连接异常, uid=" + uid);
        }
        if (uid.startsWith("fatal")) {
            throw new UnrecoverableTaskException("模拟不可恢复异常, uid=" + uid);
        }
    }

    /**
     * 启动加载：按 taskId 游标分批把「已到期且仍为 PENDING」的任务放回内存队列。
     *
     * <p>只加载 nextExecuteTime &lt;= 当前时间 的任务，退避中的重试任务不会被提前执行。</p>
     */
    private void loadPendingTasks() {
        int loaded = 0;
        String cursor = "";
        while (running) {
            List<TaskEntity> batch = taskRepository
                    .findByStatusAndNextExecuteTimeLessThanEqualAndTaskIdGreaterThanOrderByTaskIdAsc(
                            TaskStatus.PENDING, LocalDateTime.now(), cursor,
                            PageRequest.of(0, STARTUP_LOAD_BATCH));
            if (batch.isEmpty()) {
                break;
            }
            int offeredInBatch = 0;
            for (TaskEntity task : batch) {
                // 记录入队时间，用于排队时长指标
                task.setEnqueueTimeMillis(System.currentTimeMillis());
                if (offerWithRetry(task)) {
                    offeredInBatch++;
                }
            }
            loaded += offeredInBatch;
            cursor = batch.get(batch.size() - 1).getTaskId();
            // 队列持续满或已到最后一页则结束本轮加载，剩余任务留在数据库
            if (batch.size() < STARTUP_LOAD_BATCH || offeredInBatch == 0) {
                break;
            }
        }
        log.info("启动加载待执行任务完成: loaded={}, queueSize={}/{}",
                loaded, taskQueue.size(), taskQueue.getCapacity());
    }

    private boolean offerWithRetry(TaskEntity task) {
        for (int attempt = 0; attempt < OFFER_RETRY_TIMES; attempt++) {
            if (!running) {
                return false;
            }
            try {
                taskQueue.offer(task);
                return true;
            } catch (TaskQueueFullException e) {
                try {
                    Thread.sleep(OFFER_RETRY_INTERVAL_MILLIS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        log.warn("启动加载时队列持续满，任务保留在数据库: taskId={}", task.getTaskId());
        return false;
    }

    /**
     * 关闭消费线程池。
     *
     * <p>先 {@code shutdown()} 不再接收新 worker，并等待在途任务自然执行完成；
     * 超过 timeoutSeconds 仍未退出则 {@code shutdownNow()} 强制中断，
     * 被中断的任务由 {@link #process(TaskEntity)} 标记为 FAILED 并记录原因。</p>
     *
     * <p>调用前应由停机编排器先 drainAll() 并 close() 队列，空闲消费者才能立即退出。</p>
     */
    public void shutdown(long timeoutSeconds) {
        if (!shutdownStarted.compareAndSet(false, true)) {
            return;
        }
        running = false;
        ThreadPoolExecutor pool = this.workerPool;
        if (pool == null) {
            return;
        }
        log.info("开始关闭消费线程池: timeout={}s", timeoutSeconds);
        // 不中断正在执行的任务，只停止接收新的 worker 任务
        pool.shutdown();
        try {
            if (!pool.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                log.warn("消费线程未在 {}s 内结束，执行 shutdownNow 强制中断", timeoutSeconds);
                List<Runnable> dropped = pool.shutdownNow();
                log.warn("强制中断完成: 未开始执行的 worker 数={}", dropped.size());
                // 给中断分支回写任务状态留出时间
                pool.awaitTermination(3, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            log.warn("等待消费线程退出时被中断，执行 shutdownNow");
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("任务消费线程池已停止");
    }

    /**
     * 兜底关闭：正常停机由 GracefulShutdownManager 调用 {@link #shutdown(long)}，
     * 这里防止其他关闭路径漏掉线程池回收。
     */
    @PreDestroy
    public void stop() {
        shutdown(shutdownTimeoutSeconds);
    }

    /**
     * 消费线程工厂：非守护线程 + 可读线程名，便于 jstack 与日志定位。
     */
    private static class WorkerThreadFactory implements ThreadFactory {

        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "ab-task-worker-" + counter.incrementAndGet());
            thread.setDaemon(false);
            thread.setUncaughtExceptionHandler((t, e) -> log.error("消费线程未捕获异常: {}", t.getName(), e));
            return thread;
        }
    }

    /**
     * AB 任务参数结构。
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AbTaskParam(String uid, List<String> expIds) {
    }
}
