package com.example.asyncqueue.integration;

import com.example.asyncqueue.entity.TaskEntity;
import com.example.asyncqueue.entity.TaskResultEntity;
import com.example.asyncqueue.enums.TaskStatus;
import com.example.asyncqueue.repository.TaskRepository;
import com.example.asyncqueue.repository.TaskResultRepository;
import com.example.asyncqueue.service.TaskSubmitService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 任务执行流程集成测试（H2 内存库 + 真实消费线程）。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.datasource.url=jdbc:h2:mem:exec_it;DB_CLOSE_DELAY=-1",
                "async-queue.executor.worker-count=2",
                "async-queue.executor.simulate-io-millis=0",
                "async-queue.queue.capacity=64",
                "logging.level.com.example.asyncqueue=INFO"
        })
class TaskExecutionIntegrationTest {

    private static final long TIMEOUT_MILLIS = 15_000;

    @Autowired
    private TaskSubmitService taskSubmitService;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private TaskResultRepository taskResultRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("任务执行成功：状态 SUCCESS 且分桶结果落库")
    void taskShouldSucceedAndPersistResult() throws Exception {
        String uid = "u-success-" + UUID.randomUUID();
        String taskId = taskSubmitService.submitTask(
                uniqueKey(),
                "{\"uid\":\"" + uid + "\",\"expIds\":[\"test1\",\"test10\"]}",
                5, 3);

        TaskEntity task = awaitTask(taskId, t -> t.getStatus() == TaskStatus.SUCCESS);
        assertEquals(0, task.getRetryCount(), "首次执行成功不应有重试");

        TaskResultEntity result = taskResultRepository.findByTaskId(taskId)
                .orElseThrow(() -> new AssertionError("成功任务应写入结果表"));
        JsonNode array = objectMapper.readTree(result.getResultJson());
        assertEquals(2, array.size(), "两个实验应产生两条结果");

        for (JsonNode node : array) {
            String expId = node.get("expId").asText();
            int bucket = node.get("bucket").asInt();
            boolean treatment = node.get("treatment").asBoolean();
            int limit = "test1".equals(expId) ? 500 : 9500;
            assertTrue(bucket >= 0 && bucket < 10_000, "桶号应在 0~9999");
            assertEquals(bucket < limit, treatment, expId + " 的实验组判定应与桶号一致");
        }
    }

    @Test
    @DisplayName("可重试异常：未达上限时回到 PENDING，等待指数退避后的下次执行")
    void retryableFailureShouldReturnToPending() throws Exception {
        String taskId = taskSubmitService.submitTask(
                uniqueKey(),
                "{\"uid\":\"biz-retry-" + UUID.randomUUID() + "\",\"expIds\":[\"test2\"]}",
                5, 3);

        TaskEntity task = awaitTask(taskId, t -> t.getStatus() == TaskStatus.PENDING && t.getRetryCount() == 1);
        assertNotNull(task.getFailReason());
        assertTrue(task.getFailReason().contains("临时性"), "应记录业务异常原因");
        assertTrue(task.getNextExecuteTime().isAfter(LocalDateTime.now()), "nextExecuteTime 应在未来（指数退避）");

        // 重试任务不回内存队列，也不应在原进程内被立即重新执行
        Thread.sleep(700);
        TaskEntity afterWait = taskRepository.findById(taskId).orElseThrow();
        assertEquals(1, afterWait.getRetryCount(), "重试任务不应被自动重新执行");
    }

    @Test
    @DisplayName("可重试异常达到最大重试次数：进入 DEAD_LETTER")
    void retryExhaustedShouldEnterDeadLetter() throws Exception {
        String taskId = taskSubmitService.submitTask(
                uniqueKey(),
                "{\"uid\":\"biz-retry-" + UUID.randomUUID() + "\",\"expIds\":[\"test3\"]}",
                5, 0);

        TaskEntity task = awaitTask(taskId, t -> t.getStatus() == TaskStatus.DEAD_LETTER);
        assertNotNull(task.getFailReason());
        assertTrue(task.getFailReason().contains("耗尽"), "死信原因应说明重试次数耗尽: " + task.getFailReason());
    }

    @Test
    @DisplayName("不可恢复异常：直接进入 DEAD_LETTER，不消耗重试次数")
    void unrecoverableFailureShouldEnterDeadLetterDirectly() throws Exception {
        String taskId = taskSubmitService.submitTask(
                uniqueKey(),
                "{\"uid\":\"fatal-" + UUID.randomUUID() + "\",\"expIds\":[\"test4\"]}",
                5, 3);

        TaskEntity task = awaitTask(taskId, t -> t.getStatus() == TaskStatus.DEAD_LETTER);
        assertEquals(0, task.getRetryCount(), "不可恢复异常不应消耗重试次数");
        assertTrue(task.getFailReason().contains("不可恢复"), "应记录不可恢复原因: " + task.getFailReason());
    }

    private static String uniqueKey() {
        // business_key 列长度 32，与生产环境的 md5 业务键保持一致
        return UUID.randomUUID().toString().replace("-", "");
    }

    private TaskEntity awaitTask(String taskId, Predicate<TaskEntity> condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TIMEOUT_MILLIS;
        TaskEntity last = null;
        while (System.currentTimeMillis() < deadline) {
            last = taskRepository.findById(taskId).orElse(null);
            if (last != null && condition.test(last)) {
                return last;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("等待任务状态超时, taskId=" + taskId + ", 最后状态="
                + (last == null ? "null" : last.getStatus() + "/retry=" + last.getRetryCount()));
    }
}
