package com.example.asyncqueue.scheduler;

import com.example.asyncqueue.entity.TaskEntity;
import com.example.asyncqueue.enums.TaskStatus;
import com.example.asyncqueue.lifecycle.TaskSubmissionGate;
import com.example.asyncqueue.queue.BoundedPriorityTaskQueue;
import com.example.asyncqueue.queue.TaskQueueFullException;
import com.example.asyncqueue.repository.TaskRepository;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 重试 / 补偿扫描器。
 *
 * <p>解决两件事：</p>
 * <ol>
 *   <li><b>重试任务回投</b>：失败任务只写数据库（nextExecuteTime），不做内存回塞；
 *       本扫描器按到期时间把它们重新投递到内存队列；</li>
 *   <li><b>背压任务补偿</b>：提交时队列满被 deferred 的任务，同样由这里重新投递。</li>
 * </ol>
 *
 * <p>去重：投递前检查队列中是否已存在该 taskId，避免重复占位；
 * 即使重复投递，消费端 CAS 也会保证只有一个线程能执行。</p>
 */
@Slf4j
@Component
@Order(1)
public class TaskRecoveryScheduler implements ApplicationRunner {

    private final TaskRepository taskRepository;
    private final BoundedPriorityTaskQueue taskQueue;
    private final TaskSubmissionGate submissionGate;
    private final long intervalMillis;
    private final int batchSize;

    private ScheduledExecutorService executor;
    private volatile boolean running;

    public TaskRecoveryScheduler(TaskRepository taskRepository,
                                 BoundedPriorityTaskQueue taskQueue,
                                 TaskSubmissionGate submissionGate,
                                 @Value("${async-queue.scheduler.recovery-interval-millis:1000}") long intervalMillis,
                                 @Value("${async-queue.scheduler.recovery-batch-size:200}") int batchSize) {
        this.taskRepository = taskRepository;
        this.taskQueue = taskQueue;
        this.submissionGate = submissionGate;
        this.intervalMillis = intervalMillis;
        this.batchSize = batchSize;
    }

    @Override
    public void run(ApplicationArguments args) {
        running = true;
        executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "ab-task-recovery");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleWithFixedDelay(this::scanSafely, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        log.info("重试/补偿扫描器已启动: interval={}ms, batch={}", intervalMillis, batchSize);
    }

    private void scanSafely() {
        try {
            scanOnce();
        } catch (Exception e) {
            // 扫描失败不能影响后续轮次
            log.error("重试/补偿扫描异常", e);
        }
    }

    private void scanOnce() {
        if (!running || !submissionGate.isAccepting()) {
            return;
        }
        List<TaskEntity> candidates = taskRepository
                .findByStatusAndNextExecuteTimeLessThanEqualOrderByPriorityDescCreateTimeAsc(
                        TaskStatus.PENDING, LocalDateTime.now(), PageRequest.of(0, batchSize));
        int offered = 0;
        for (TaskEntity task : candidates) {
            if (taskQueue.contains(task.getTaskId())) {
                continue;
            }
            task.setEnqueueTimeMillis(System.currentTimeMillis());
            try {
                taskQueue.offer(task);
                offered++;
            } catch (TaskQueueFullException e) {
                log.debug("补偿投递时队列已满，等待下一轮: capacity={}", e.getCapacity());
                break;
            } catch (IllegalStateException e) {
                // 队列已关闭（停机中）
                return;
            }
        }
        if (offered > 0) {
            log.info("重试/补偿扫描投递任务: offered={}", offered);
        }
    }

    /**
     * 停机时先于队列回写停止扫描，避免回写后的任务又被重新投递。
     */
    @PreDestroy
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (executor != null) {
            executor.shutdownNow();
        }
        log.info("重试/补偿扫描器已停止");
    }
}
