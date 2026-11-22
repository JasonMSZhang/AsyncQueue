package com.example.asyncqueue.service;

import com.example.asyncqueue.entity.TaskEntity;
import com.example.asyncqueue.enums.TaskStatus;
import com.example.asyncqueue.queue.BoundedPriorityTaskQueue;
import com.example.asyncqueue.queue.TaskQueueFullException;
import com.example.asyncqueue.repository.TaskRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * 任务提交服务。
 *
 * <p><b>核心顺序（不可调整）：</b></p>
 * <ol>
 *   <li>按 businessKey 查询未结束任务，命中则直接复用，保证幂等；</li>
 *   <li>新任务先写入 H2 并<b>提交事务</b>；</li>
 *   <li>提交成功后，才尝试放入内存队列。</li>
 * </ol>
 *
 * <p>任何时刻都允许内存队列入队失败（{@link TaskQueueFullException}），因为任务已经落库；
 * 绝不允许先入内存再落库，否则进程宕机会导致任务丢失。</p>
 */
@Slf4j
@Service
public class TaskSubmitService {

    private final TaskRepository taskRepository;

    private final BoundedPriorityTaskQueue taskQueue;

    /**
     * 专用于「落库」的事务模板。
     *
     * <p>使用 REQUIRES_NEW 是为了确保 insert 在方法返回前就完成提交，
     * 不受调用方是否已开启事务影响；否则可能出现「事务未提交就已经入内存队列」。</p>
     */
    private final TransactionTemplate insertTransactionTemplate;

    public TaskSubmitService(TaskRepository taskRepository,
                             BoundedPriorityTaskQueue taskQueue,
                             PlatformTransactionManager transactionManager) {
        this.taskRepository = taskRepository;
        this.taskQueue = taskQueue;
        this.insertTransactionTemplate = new TransactionTemplate(transactionManager);
        this.insertTransactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    /**
     * 提交 AB-Test 分桶任务。
     *
     * @param businessKey 幂等业务键（如 uid + 排序后的实验ID列表的 md5）
     * @param paramJson   AB 任务入参 JSON（uid、expId 列表等）
     * @param priority    优先级，数值越大越先执行
     * @param maxRetry    最大重试次数
     * @return 任务ID；若已有未结束任务，返回该任务已有ID
     * @throws TaskQueueFullException 任务已落库，但内存队列已满（背压）
     */
    public String submitTask(String businessKey, String paramJson, int priority, int maxRetry) {
        Assert.hasText(businessKey, "businessKey 不能为空");
        Assert.hasText(paramJson, "paramJson 不能为空");
        Assert.isTrue(maxRetry >= 0, "maxRetry 不能小于 0");

        // 1. 幂等判断：PENDING / PROCESSING 属于未结束，直接复用已有任务
        Optional<TaskEntity> unfinished = taskRepository.findUnfinishedByBusinessKey(businessKey);
        if (unfinished.isPresent()) {
            TaskEntity existing = unfinished.get();
            log.info("任务已存在，复用已有任务: businessKey={}, taskId={}, status={}",
                    businessKey, existing.getTaskId(), existing.getStatus());
            return existing.getTaskId();
        }

        // 2. 先持久化：事务提交成功后，任务才算真正被接收
        TaskEntity task = buildNewTask(businessKey, paramJson, priority, maxRetry);
        TaskEntity saved = Objects.requireNonNull(
                insertTransactionTemplate.execute(status -> taskRepository.save(task)),
                "任务持久化失败，事务已回滚");
        log.info("任务已持久化: taskId={}, businessKey={}, priority={}", saved.getTaskId(), businessKey, priority);

        // 3. 再入内存队列；此处失败只影响执行时延，不影响任务是否被记住
        try {
            taskQueue.offer(saved);
            log.info("任务入队成功: taskId={}, queueSize={}/{}",
                    saved.getTaskId(), taskQueue.size(), taskQueue.getCapacity());
        } catch (TaskQueueFullException e) {
            // 任务已在库中（PENDING），客户端可用同一 businessKey 重试，届时会命中幂等查询拿回该 taskId
            log.warn("内存队列已满，任务暂缓执行: taskId={}, businessKey={}, capacity={}, currentSize={}",
                    saved.getTaskId(), businessKey, e.getCapacity(), e.getSize());
            throw e;
        }
        return saved.getTaskId();
    }

    private TaskEntity buildNewTask(String businessKey, String paramJson, int priority, int maxRetry) {
        LocalDateTime now = LocalDateTime.now();
        return TaskEntity.builder()
                .taskId(UUID.randomUUID().toString().replace("-", ""))
                .businessKey(businessKey)
                .priority(priority)
                .taskParamJson(paramJson)
                .status(TaskStatus.PENDING)
                .retryCount(0)
                .maxRetry(maxRetry)
                .nextExecuteTime(now)
                .createTime(now)
                .updateTime(now)
                .build();
    }
}
