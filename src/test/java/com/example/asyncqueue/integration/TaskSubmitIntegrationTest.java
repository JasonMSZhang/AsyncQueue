package com.example.asyncqueue.integration;

import com.example.asyncqueue.entity.TaskEntity;
import com.example.asyncqueue.enums.TaskStatus;
import com.example.asyncqueue.lifecycle.TaskSubmissionGate;
import com.example.asyncqueue.queue.BoundedPriorityTaskQueue;
import com.example.asyncqueue.queue.TaskQueueFullException;
import com.example.asyncqueue.repository.TaskRepository;
import com.example.asyncqueue.service.TaskSubmitService;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 任务提交相关集成测试（H2 内存库）。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.datasource.url=jdbc:h2:mem:submit_it;DB_CLOSE_DELAY=-1",
                "async-queue.executor.worker-count=2",
                "async-queue.executor.simulate-io-millis=0",
                "async-queue.queue.capacity=64",
                "logging.level.com.example.asyncqueue=INFO"
        })
class TaskSubmitIntegrationTest {

    @Autowired
    private TaskSubmitService taskSubmitService;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private TaskSubmissionGate submissionGate;

    @Test
    @DisplayName("相同 businessKey 重复提交返回同一个 taskId，且只落一条记录")
    void submitSameBusinessKeyShouldReturnSameTaskId() {
        String businessKey = uniqueKey();
        // uid 以 biz-retry 开头：任务会失败并停留在 PENDING，保证两次提交期间任务都未结束
        String paramJson = "{\"uid\":\"biz-retry-idem-" + UUID.randomUUID() + "\",\"expIds\":[\"test1\"]}";

        String firstTaskId = taskSubmitService.submitTask(businessKey, paramJson, 5, 3);
        String secondTaskId = taskSubmitService.submitTask(businessKey, paramJson, 5, 3);

        assertEquals(firstTaskId, secondTaskId, "幂等提交应返回相同 taskId");
        long persisted = taskRepository.findAll().stream()
                .filter(task -> businessKey.equals(task.getBusinessKey()))
                .count();
        assertEquals(1, persisted, "相同 businessKey 只应落一条任务");
    }

    @Test
    @DisplayName("内存队列满时抛 TaskQueueFullException，且任务已经持久化为 PENDING")
    void queueFullShouldThrowAndKeepTaskPersisted() {
        BoundedPriorityTaskQueue tinyQueue = new BoundedPriorityTaskQueue(1);
        tinyQueue.offer(dummyTask("occupier-" + UUID.randomUUID()));

        TaskSubmitService serviceWithTinyQueue = new TaskSubmitService(
                taskRepository, tinyQueue, submissionGate, transactionManager, meterRegistry);

        String businessKey = uniqueKey();
        String paramJson = "{\"uid\":\"u-full-" + UUID.randomUUID() + "\",\"expIds\":[\"test1\"]}";

        assertThrows(TaskQueueFullException.class,
                () -> serviceWithTinyQueue.submitTask(businessKey, paramJson, 5, 3),
                "队列满应触发背压异常");

        TaskEntity persisted = taskRepository.findUnfinishedByBusinessKey(businessKey)
                .orElseThrow(() -> new AssertionError("背压时任务应已落库"));
        assertEquals(TaskStatus.PENDING, persisted.getStatus(), "未入队的任务应保持 PENDING");
        assertEquals(1, tinyQueue.size(), "背压任务不应进入内存队列");
    }

    @Test
    @DisplayName("多线程并发提交不同 businessKey，任务全部成功落库")
    void concurrentSubmitShouldPersistAllTasks() throws Exception {
        int threads = 10;
        String runId = UUID.randomUUID().toString().replace("-", "");
        List<String> businessKeys = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            businessKeys.add(uniqueKey());
        }

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        List<Future<String>> futures = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            final int index = i;
            futures.add(pool.submit(() -> {
                startGate.await();
                String paramJson = "{\"uid\":\"u-conc-" + runId + "-" + index
                        + "\",\"expIds\":[\"test1\",\"test2\"]}";
                return taskSubmitService.submitTask(businessKeys.get(index), paramJson, 5, 3);
            }));
        }

        startGate.countDown();
        Set<String> taskIds = new HashSet<>();
        for (Future<String> future : futures) {
            taskIds.add(future.get(20, TimeUnit.SECONDS));
        }
        pool.shutdownNow();

        assertEquals(threads, taskIds.size(), "并发提交应生成互不相同的 taskId");
        long persisted = taskRepository.findAll().stream()
                .filter(task -> businessKeys.contains(task.getBusinessKey()))
                .count();
        assertEquals(threads, persisted, "并发提交的任务应全部落库");
    }

    private static String uniqueKey() {
        // business_key 列长度 32，与生产环境的 md5 业务键保持一致
        return UUID.randomUUID().toString().replace("-", "");
    }

    private TaskEntity dummyTask(String taskId) {
        LocalDateTime now = LocalDateTime.now();
        return TaskEntity.builder()
                .taskId(taskId)
                .businessKey("bk-" + taskId)
                .priority(1)
                .taskParamJson("{}")
                .status(TaskStatus.PENDING)
                .retryCount(0)
                .maxRetry(3)
                .nextExecuteTime(now)
                .createTime(now)
                .updateTime(now)
                .build();
    }
}
