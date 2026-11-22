package com.example.asyncqueue.metrics;

import com.example.asyncqueue.executor.TaskExecutor;
import com.example.asyncqueue.queue.BoundedPriorityTaskQueue;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * 队列与执行器相关 Gauge 指标绑定。
 *
 * <p>Gauge 持有的是单例 Bean 的弱引用，可随时反映运行时实时值。</p>
 */
@Component
public class AsyncQueueMetrics {

    public AsyncQueueMetrics(MeterRegistry meterRegistry,
                             BoundedPriorityTaskQueue taskQueue,
                             TaskExecutor taskExecutor) {
        Gauge.builder("async.queue.size", taskQueue, BoundedPriorityTaskQueue::size)
                .description("内存优先级队列当前任务数")
                .register(meterRegistry);

        Gauge.builder("async.queue.capacity", taskQueue, BoundedPriorityTaskQueue::getCapacity)
                .description("内存优先级队列容量")
                .register(meterRegistry);

        Gauge.builder("async.executor.active.threads", taskExecutor, TaskExecutor::getBusyThreadCount)
                .description("当前正在执行任务的消费线程数")
                .register(meterRegistry);
    }
}
