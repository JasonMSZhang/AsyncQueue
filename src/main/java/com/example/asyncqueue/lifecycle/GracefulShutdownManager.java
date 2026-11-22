package com.example.asyncqueue.lifecycle;

import com.example.asyncqueue.entity.TaskEntity;
import com.example.asyncqueue.executor.TaskExecutor;
import com.example.asyncqueue.queue.BoundedPriorityTaskQueue;
import com.example.asyncqueue.service.TaskStateService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 优雅停机编排器。
 *
 * <p>严格按以下顺序执行，顺序不可交换：</p>
 * <ol>
 *   <li>关闭提交开关，拒绝新任务；</li>
 *   <li>{@code drainAll()} 导出内存中未消费任务，批量回写数据库为 PENDING；</li>
 *   <li>关闭队列入口并唤醒空闲消费者；</li>
 *   <li>关闭消费线程池，等待在途任务完成，超时则强制中断（任务标记 FAILED）。</li>
 * </ol>
 *
 * <p>{@link #getPhase()} 返回最大值，保证本组件在 Web 容器、JPA 等基础设施关闭之前执行，
 * 否则回写数据库会失败。</p>
 */
@Slf4j
@Component
public class GracefulShutdownManager implements SmartLifecycle {

    private final TaskSubmissionGate submissionGate;
    private final BoundedPriorityTaskQueue taskQueue;
    private final TaskStateService taskStateService;
    private final TaskExecutor taskExecutor;
    private final long shutdownTimeoutSeconds;

    /** 防止 stop() 与 stop(callback) 重复执行 */
    private final AtomicBoolean shutdownDone = new AtomicBoolean(false);

    private volatile boolean running;

    public GracefulShutdownManager(TaskSubmissionGate submissionGate,
                                   BoundedPriorityTaskQueue taskQueue,
                                   TaskStateService taskStateService,
                                   TaskExecutor taskExecutor,
                                   @Value("${async-queue.executor.shutdown-timeout-seconds:20}") long shutdownTimeoutSeconds) {
        this.submissionGate = submissionGate;
        this.taskQueue = taskQueue;
        this.taskStateService = taskStateService;
        this.taskExecutor = taskExecutor;
        this.shutdownTimeoutSeconds = shutdownTimeoutSeconds;
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        doShutdown();
        running = false;
    }

    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            // 必须回调，否则 Spring 会一直等待生命周期停止
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * 最大值：先于其他生命周期组件停止（Web 容器、JPA、数据源）。停机流程本身是同步的，
     * Spring 会等待 stop 回调后再继续关闭应用上下文。
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    private void doShutdown() {
        if (!shutdownDone.compareAndSet(false, true)) {
            return;
        }
        log.info("开始优雅停机: shutdownTimeout={}s", shutdownTimeoutSeconds);

        // 1. 关闭全局开关：拒绝新提交（上层转换为 503）
        submissionGate.stopAccepting();
        log.info("已停止接收新任务");

        // 2. 导出内存队列中还未消费的任务，并批量回写为 PENDING
        List<TaskEntity> undrainedTasks = taskQueue.drainAll();
        int writtenBack = taskStateService.resetToPending(
                undrainedTasks.stream().map(TaskEntity::getTaskId).toList());
        log.info("内存队列任务回写完成: drained={}, updated={}", undrainedTasks.size(), writtenBack);

        // 3. 关闭队列入口并唤醒空闲消费者（队列中已无任务，take() 将返回 null）
        taskQueue.close();

        // 4. 关闭线程池：先允许在途任务跑完，超时再强制中断
        taskExecutor.shutdown(shutdownTimeoutSeconds);

        log.info("优雅停机完成");
    }
}
