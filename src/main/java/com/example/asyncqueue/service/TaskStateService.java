package com.example.asyncqueue.service;

import com.example.asyncqueue.entity.TaskEntity;
import com.example.asyncqueue.entity.TaskResultEntity;
import com.example.asyncqueue.enums.TaskStatus;
import com.example.asyncqueue.repository.TaskRepository;
import com.example.asyncqueue.repository.TaskResultRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * 任务状态流转服务：把「状态变更 + 结果写入」收敛在事务内，避免出现中间态。
 *
 * <p>所有方法都假定调用方是任务的当前持有者（已成功 CAS 到 PROCESSING）。</p>
 */
@Slf4j
@Service
public class TaskStateService {

    /** fail_reason 字段长度上限，超长截断，完整堆栈只进日志 */
    private static final int FAIL_REASON_MAX_LENGTH = 512;

    /** 批量 UPDATE 的单批大小，避免 IN 列表过长 */
    private static final int BATCH_UPDATE_SIZE = 500;

    private final TaskRepository taskRepository;
    private final TaskResultRepository taskResultRepository;
    private final long retryBaseDelaySeconds;
    private final long retryMaxDelaySeconds;

    /** 重试计数：每次安排重试累加 */
    private final Counter retryCounter;

    public TaskStateService(TaskRepository taskRepository,
                            TaskResultRepository taskResultRepository,
                            MeterRegistry meterRegistry,
                            @Value("${async-queue.retry.base-delay-seconds:1}") long retryBaseDelaySeconds,
                            @Value("${async-queue.retry.max-delay-seconds:60}") long retryMaxDelaySeconds) {
        this.taskRepository = taskRepository;
        this.taskResultRepository = taskResultRepository;
        this.retryBaseDelaySeconds = retryBaseDelaySeconds;
        this.retryMaxDelaySeconds = retryMaxDelaySeconds;
        this.retryCounter = Counter.builder("async.task.retry")
                .description("任务安排重试的总次数")
                .register(meterRegistry);
    }

    /**
     * 领取任务：PENDING -> PROCESSING（CAS）。
     *
     * @return true 表示领取成功；false 表示任务已被其他线程处理或状态已变化
     */
    @Transactional
    public boolean markProcessing(String taskId) {
        int updated = taskRepository.casStatus(taskId, TaskStatus.PENDING, TaskStatus.PROCESSING, LocalDateTime.now());
        return updated == 1;
    }

    /**
     * 执行成功：写入结果表 + 任务置 SUCCESS，同一事务内完成。
     *
     * <p>若结果已存在（历史重复执行），直接覆盖结果内容，保证 1:1 唯一约束不被破坏。</p>
     */
    @Transactional
    public void markSuccess(String taskId, String resultJson) {
        TaskEntity task = taskRepository.findById(taskId).orElse(null);
        if (task == null || task.getStatus() != TaskStatus.PROCESSING) {
            log.warn("任务状态非 PROCESSING，跳过成功回写: taskId={}", taskId);
            return;
        }

        TaskResultEntity result = taskResultRepository.findByTaskId(taskId)
                .orElseGet(() -> TaskResultEntity.builder()
                        .id(UUID.randomUUID().toString().replace("-", ""))
                        .taskId(taskId)
                        .build());
        result.setResultJson(resultJson);
        result.setCompleteTime(LocalDateTime.now());
        taskResultRepository.save(result);

        task.setStatus(TaskStatus.SUCCESS);
        task.setFailReason(null);
        taskRepository.save(task);
        log.info("任务执行成功: taskId={}", taskId);
    }

    /**
     * 执行失败处理。
     *
     * <ul>
     *   <li>可重试且未超过 maxRetry：retryCount+1，状态回 PENDING，按指数退避设置 nextExecuteTime；
     *       任务只留在数据库，<b>不重新入内存队列</b>；</li>
     *   <li>不可重试或重试耗尽：状态置 DEAD_LETTER，记录 failReason。</li>
     * </ul>
     */
    @Transactional
    public void handleFailure(String taskId, String failReason, boolean retryable) {
        TaskEntity task = taskRepository.findById(taskId).orElse(null);
        if (task == null || task.getStatus() != TaskStatus.PROCESSING) {
            log.warn("任务状态非 PROCESSING，跳过失败回写: taskId={}", taskId);
            return;
        }

        if (!retryable) {
            markDeadLetter(task, failReason);
            return;
        }

        int currentRetry = task.getRetryCount() == null ? 0 : task.getRetryCount();
        int maxRetry = task.getMaxRetry() == null ? 0 : task.getMaxRetry();
        if (currentRetry >= maxRetry) {
            markDeadLetter(task, "重试次数已耗尽(已重试" + currentRetry + "次): " + failReason);
            return;
        }

        // 指数退避：第 1 次失败等待 base*1，第 2 次 base*2，第 3 次 base*4 …… 上限 maxDelay
        int nextRetry = currentRetry + 1;
        long delaySeconds = backoffSeconds(currentRetry);
        task.setRetryCount(nextRetry);
        task.setStatus(TaskStatus.PENDING);
        task.setNextExecuteTime(LocalDateTime.now().plusSeconds(delaySeconds));
        task.setFailReason(truncate(failReason));
        taskRepository.save(task);
        retryCounter.increment();
        log.warn("任务失败，安排第 {} 次重试: taskId={}, delay={}s, nextExecuteTime={}, reason={}",
                nextRetry, taskId, delaySeconds, task.getNextExecuteTime(), failReason);
    }

    /**
     * 停机回写：把内存队列里尚未消费的任务批量置回 PENDING，保证重启后能被重新加载执行。
     *
     * <p>这些任务在数据库中本来就是 PENDING，此处的批量更新是显式收敛状态与更新时间，
     * 也防止停机前发生过状态漂移。</p>
     *
     * @param taskIds 待回写任务ID
     * @return 实际更新的行数
     */
    @Transactional
    public int resetToPending(Collection<String> taskIds) {
        if (taskIds == null || taskIds.isEmpty()) {
            return 0;
        }
        List<String> ids = new ArrayList<>(taskIds);
        LocalDateTime now = LocalDateTime.now();
        int updated = 0;
        for (int from = 0; from < ids.size(); from += BATCH_UPDATE_SIZE) {
            int to = Math.min(from + BATCH_UPDATE_SIZE, ids.size());
            updated += taskRepository.updateStatusByTaskIds(ids.subList(from, to), TaskStatus.PENDING, now);
        }
        return updated;
    }

    /**
     * 停机超时强制中断：把正在执行的任务标记为 FAILED 并记录原因。
     *
     * <p>与可重试失败不同，这里不重新排期：强制中断往往意味着任务本身过慢，
     * 继续自动重试容易在下次停机时重复触发。</p>
     */
    @Transactional
    public void markFailed(String taskId, String failReason) {
        TaskEntity task = taskRepository.findById(taskId).orElse(null);
        if (task == null || task.getStatus() != TaskStatus.PROCESSING) {
            return;
        }
        task.setStatus(TaskStatus.FAILED);
        task.setFailReason(truncate(failReason));
        taskRepository.save(task);
        log.error("任务执行被强制中断，标记为失败: taskId={}, reason={}", taskId, failReason);
    }

    private void markDeadLetter(TaskEntity task, String failReason) {
        task.setStatus(TaskStatus.DEAD_LETTER);
        task.setFailReason(truncate(failReason));
        taskRepository.save(task);
        log.error("任务进入死信: taskId={}, retryCount={}, reason={}",
                task.getTaskId(), task.getRetryCount(), failReason);
    }

    /**
     * 指数退避秒数：base * 2^retryCount，超过上限则取上限。
     * 位移次数做上限保护，避免整数溢出。
     */
    private long backoffSeconds(int retryCount) {
        long delay = retryBaseDelaySeconds * (1L << Math.min(retryCount, 16));
        return Math.min(delay, retryMaxDelaySeconds);
    }

    private String truncate(String reason) {
        if (reason == null) {
            return null;
        }
        return reason.length() <= FAIL_REASON_MAX_LENGTH
                ? reason
                : reason.substring(0, FAIL_REASON_MAX_LENGTH);
    }
}
