package com.example.asyncqueue.service;

import com.example.asyncqueue.entity.TaskEntity;
import com.example.asyncqueue.entity.TaskResultEntity;
import com.example.asyncqueue.enums.TaskStatus;
import com.example.asyncqueue.repository.TaskRepository;
import com.example.asyncqueue.repository.TaskResultRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
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

    private final TaskRepository taskRepository;
    private final TaskResultRepository taskResultRepository;
    private final long retryBaseDelaySeconds;
    private final long retryMaxDelaySeconds;

    public TaskStateService(TaskRepository taskRepository,
                            TaskResultRepository taskResultRepository,
                            @Value("${async-queue.retry.base-delay-seconds:1}") long retryBaseDelaySeconds,
                            @Value("${async-queue.retry.max-delay-seconds:60}") long retryMaxDelaySeconds) {
        this.taskRepository = taskRepository;
        this.taskResultRepository = taskResultRepository;
        this.retryBaseDelaySeconds = retryBaseDelaySeconds;
        this.retryMaxDelaySeconds = retryMaxDelaySeconds;
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
        log.warn("任务失败，安排第 {} 次重试: taskId={}, delay={}s, nextExecuteTime={}, reason={}",
                nextRetry, taskId, delaySeconds, task.getNextExecuteTime(), failReason);
    }

    /**
     * 停机/中断回收：PROCESSING -> PENDING，不消耗重试次数。
     * 任务不会被直接放回内存队列，等待下次启动加载或补偿扫描。
     */
    @Transactional
    public void releaseToPending(String taskId, String reason) {
        TaskEntity task = taskRepository.findById(taskId).orElse(null);
        if (task == null || task.getStatus() != TaskStatus.PROCESSING) {
            return;
        }
        task.setStatus(TaskStatus.PENDING);
        task.setNextExecuteTime(LocalDateTime.now());
        task.setFailReason(truncate(reason));
        taskRepository.save(task);
        log.info("任务已回置为待执行: taskId={}, reason={}", taskId, reason);
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
