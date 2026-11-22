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
    @DisplayName("可重试异常：指数退避后由补偿扫描器自动重试，最终成功")
    void retryableFailureShouldEventuallySucceed() throws Exception {
        String taskId = taskSubmitService.submitTask(
                uniqueKey(),
                "{\"uid\":\"biz-retry-" + UUID.randomUUID() + "\",\"expIds\":[\"test2\"]}",
                5, 3);

        // 第一次失败后回到 PENDING，并排定未来执行时间（指数退避）
        TaskEntity pending = awaitTask(taskId,
                t -> t.getStatus() == TaskStatus.PENDING && t.getRetryCount() == 1);
        assertNotNull(pending.getFailReason());
        assertTrue(pending.getFailReason().contains("临时性"), "应记录业务异常原因");
        assertTrue(pending.getNextExecuteTime().isAfter(LocalDateTime.now()), "nextExecuteTime 应在未来（指数退避）");

        // 补偿扫描器到期后重新投递，第二次重试成功（模拟故障在 retryCount >= 2 后不再触发）
        TaskEntity done = awaitTask(taskId, t -> t.getStatus() == TaskStatus.SUCCESS);
        assertEquals(2, done.getRetryCount(), "应在两次重试后成功");
        assertNotNull(taskResultRepository.findByTaskId(taskId).orElse(null), "成功任务应有关联结果");
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
