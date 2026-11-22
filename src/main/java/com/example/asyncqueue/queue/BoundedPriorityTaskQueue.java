package com.example.asyncqueue.queue;

import com.example.asyncqueue.entity.TaskEntity;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 自研有界优先级内存队列。
 *
 * <p>实现要点：</p>
 * <ul>
 *   <li>底层使用 {@link ReentrantLock} + {@link Condition}，内部用一个有序 {@link ArrayList}
 *       维护任务顺序，<b>不使用</b> {@code PriorityBlockingQueue} 等并发容器；</li>
 *   <li>排序规则：优先级高的在前；同优先级按创建时间升序（先提交先执行）；
 *       再按 taskId 兜底，保证比较器是全序、插入位置稳定；</li>
 *   <li>有界 + 快速失败：容量满时直接抛 {@link TaskQueueFullException}，即背压策略，
 *       生产者线程不会被阻塞；</li>
 *   <li>多生产者 / 多消费者安全：所有状态访问都在同一把锁内完成，内部列表不对外暴露。</li>
 * </ul>
 *
 * <p>注意：任务入队后不要再修改 priority / createTime / taskId，否则会破坏内部有序性。</p>
 */
@Component
public class BoundedPriorityTaskQueue {

    /**
     * 任务顺序比较器。
     * priority 为 null 的排最后（正常数据不应出现），其余同优先级按 createTime 升序。
     */
    private static final Comparator<TaskEntity> TASK_ORDER = Comparator
            .comparing(TaskEntity::getPriority, Comparator.nullsLast(Comparator.<Integer>reverseOrder()))
            .thenComparing(TaskEntity::getCreateTime, Comparator.nullsLast(Comparator.<LocalDateTime>naturalOrder()))
            .thenComparing(TaskEntity::getTaskId, Comparator.nullsLast(Comparator.<String>naturalOrder()));

    /** 队列容量上限（构造后不可变） */
    private final int capacity;

    /** 有序任务列表，索引 0 即为下一个要被消费的任务；仅允许持锁访问 */
    private final List<TaskEntity> tasks;

    /** 保护 tasks 以及 size/capacity 判断的独占锁 */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * 非空条件：消费者在此等待。
     *
     * <p>没有 notFull 条件，因为生产者采用「满即抛异常」的快速失败背压，
     * 不会在队列满时等待，避免占用请求线程。</p>
     */
    private final Condition notEmpty = lock.newCondition();

    public BoundedPriorityTaskQueue(@Value("${async-queue.queue.capacity:4096}") int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("队列容量必须大于 0, 当前值: " + capacity);
        }
        this.capacity = capacity;
        // 初始容量不必一次到位，避免启动即分配大数组
        this.tasks = new ArrayList<>(Math.min(capacity, 1024));
    }

    /**
     * 入队（非阻塞）。
     *
     * <p>队列满时抛 {@link TaskQueueFullException} 触发背压；成功返回 true。
     * 返回值保留 boolean 是为了与常见队列 API 保持一致，调用方也可直接忽略。</p>
     *
     * @param task 任务，不能为 null
     * @return 入队成功返回 true
     * @throws TaskQueueFullException 队列已满
     */
    public boolean offer(TaskEntity task) {
        Objects.requireNonNull(task, "任务不能为 null");
        lock.lock();
        try {
            if (tasks.size() >= capacity) {
                throw new TaskQueueFullException(capacity, tasks.size());
            }
            // 二分查找插入点，保持列表按 TASK_ORDER 有序；插入为 O(n) 内存移动，容量 4096 级别可接受
            int index = Collections.binarySearch(tasks, task, TASK_ORDER);
            if (index < 0) {
                index = -index - 1;
            }
            tasks.add(index, task);
            // 只唤醒一个消费者，减少无效竞争
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 阻塞取出优先级最高的任务。
     *
     * @return 任务实体
     * @throws InterruptedException 等待期间线程被中断（如优雅停机）
     */
    public TaskEntity take() throws InterruptedException {
        // 使用可中断加锁，保证停机时阻塞在锁上的消费者也能及时退出
        lock.lockInterruptibly();
        try {
            while (tasks.isEmpty()) {
                notEmpty.await();
            }
            return tasks.remove(0);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 当前队列中的任务数量。
     */
    public int size() {
        lock.lock();
        try {
            return tasks.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 队列容量。
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * 队列是否已满，生产者可据此提前判断，避免走异常路径。
     */
    public boolean isFull() {
        lock.lock();
        try {
            return tasks.size() >= capacity;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 导出并清空队列中的全部任务，用于优雅停机时回写数据库。
     *
     * <p>返回的是快照副本，顺序与消费顺序一致（优先级由高到低）；
     * 导出后队列为空，消费者将进入等待。</p>
     */
    public List<TaskEntity> drainAll() {
        lock.lock();
        try {
            if (tasks.isEmpty()) {
                return Collections.emptyList();
            }
            List<TaskEntity> snapshot = new ArrayList<>(tasks);
            tasks.clear();
            return snapshot;
        } finally {
            lock.unlock();
        }
    }
}
