package com.example.asyncqueue.queue;

import com.example.asyncqueue.entity.TaskEntity;
import com.example.asyncqueue.enums.TaskStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 自研有界优先级队列单元测试（不依赖 Spring）。
 */
class BoundedPriorityTaskQueueTest {

    private static final LocalDateTime BASE_TIME = LocalDateTime.of(2026, 1, 1, 0, 0);

    private static TaskEntity task(String id, int priority, LocalDateTime createTime) {
        return TaskEntity.builder()
                .taskId(id)
                .businessKey("bk-" + id)
                .priority(priority)
                .taskParamJson("{}")
                .status(TaskStatus.PENDING)
                .retryCount(0)
                .maxRetry(3)
                .nextExecuteTime(createTime)
                .createTime(createTime)
                .updateTime(createTime)
                .build();
    }

    @Test
    @DisplayName("队列满时 offer 抛 TaskQueueFullException，且不破坏已有元素")
    void offerShouldThrowWhenFull() {
        BoundedPriorityTaskQueue queue = new BoundedPriorityTaskQueue(2);
        queue.offer(task("a", 1, BASE_TIME));
        queue.offer(task("b", 2, BASE_TIME));

        TaskQueueFullException ex = assertThrows(TaskQueueFullException.class,
                () -> queue.offer(task("c", 3, BASE_TIME)));
        assertEquals(2, ex.getCapacity());
        assertEquals(2, ex.getSize());
        assertEquals(2, queue.size());
        assertTrue(queue.isFull());
    }

    @Test
    @DisplayName("take 按优先级从高到低返回")
    void takeShouldReturnHighestPriorityFirst() throws Exception {
        BoundedPriorityTaskQueue queue = new BoundedPriorityTaskQueue(10);
        queue.offer(task("low", 1, BASE_TIME));
        queue.offer(task("high", 9, BASE_TIME));
        queue.offer(task("mid", 5, BASE_TIME));

        assertEquals(9, queue.take().getPriority());
        assertEquals(5, queue.take().getPriority());
        assertEquals(1, queue.take().getPriority());
        assertEquals(0, queue.size());
    }

    @Test
    @DisplayName("同优先级按创建时间先入先出")
    void samePriorityShouldBeFifo() throws Exception {
        BoundedPriorityTaskQueue queue = new BoundedPriorityTaskQueue(10);
        queue.offer(task("later", 5, BASE_TIME.plusSeconds(2)));
        queue.offer(task("earlier", 5, BASE_TIME));

        assertEquals("earlier", queue.take().getTaskId());
        assertEquals("later", queue.take().getTaskId());
    }

    @Test
    @DisplayName("drainAll 按消费顺序导出全部任务并清空队列")
    void drainAllShouldReturnOrderedSnapshotAndClear() throws Exception {
        BoundedPriorityTaskQueue queue = new BoundedPriorityTaskQueue(10);
        queue.offer(task("low", 1, BASE_TIME));
        queue.offer(task("high", 9, BASE_TIME));
        queue.offer(task("mid", 5, BASE_TIME));

        List<TaskEntity> drained = queue.drainAll();
        assertEquals(3, drained.size());
        assertEquals("high", drained.get(0).getTaskId());
        assertEquals("mid", drained.get(1).getTaskId());
        assertEquals("low", drained.get(2).getTaskId());
        assertEquals(0, queue.size());
        assertTrue(queue.drainAll().isEmpty());
    }

    @Test
    @DisplayName("关闭后 take 返回 null，offer 被拒绝")
    void closeShouldWakeConsumersAndRejectOffers() throws Exception {
        BoundedPriorityTaskQueue queue = new BoundedPriorityTaskQueue(2);
        queue.close();

        assertTrue(queue.isClosed());
        assertNull(queue.take());
        assertThrows(IllegalStateException.class, () -> queue.offer(task("x", 1, BASE_TIME)));
    }

    @Test
    @DisplayName("多生产者多消费者并发下不丢任务、不重复消费")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentProducersAndConsumersShouldNotLoseTasks() throws Exception {
        int producers = 4;
        int consumers = 4;
        int perProducer = 500;
        int total = producers * perProducer;
        BoundedPriorityTaskQueue queue = new BoundedPriorityTaskQueue(256);

        ExecutorService producerPool = Executors.newFixedThreadPool(producers);
        CountDownLatch startGate = new CountDownLatch(1);
        List<Future<?>> producerFutures = new ArrayList<>();

        for (int p = 0; p < producers; p++) {
            final int producerId = p;
            Callable<Void> producer = () -> {
                startGate.await();
                for (int i = 0; i < perProducer; i++) {
                    TaskEntity task = task("p" + producerId + "-" + i, i % 10, BASE_TIME.plusNanos(i));
                    while (true) {
                        try {
                            queue.offer(task);
                            break;
                        } catch (TaskQueueFullException e) {
                            // 模拟背压：稍等后重试
                            Thread.sleep(1);
                        }
                    }
                }
                return null;
            };
            producerFutures.add(producerPool.submit(producer));
        }

        Set<String> consumed = ConcurrentHashMap.newKeySet();
        List<Thread> consumerThreads = new ArrayList<>();
        for (int c = 0; c < consumers; c++) {
            Thread consumer = new Thread(() -> {
                try {
                    while (true) {
                        TaskEntity task = queue.take();
                        if (task == null) {
                            return;
                        }
                        consumed.add(task.getTaskId());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "test-consumer-" + c);
            consumer.start();
            consumerThreads.add(consumer);
        }

        startGate.countDown();
        for (Future<?> future : producerFutures) {
            future.get(20, TimeUnit.SECONDS);
        }

        long deadline = System.currentTimeMillis() + 15_000;
        while (consumed.size() < total && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }

        queue.close();
        for (Thread consumer : consumerThreads) {
            consumer.join(3000);
        }
        producerPool.shutdownNow();

        assertEquals(total, consumed.size(), "消费数量应等于生产数量（无丢失、无重复）");
        assertEquals(0, queue.size());
    }
}
