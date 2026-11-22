package com.example.asyncqueue.queue;

import com.example.asyncqueue.entity.TaskEntity;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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

    /** 当前在队列中的 taskId 集合，供补偿扫描器去重，避免同一任务被重复投递；仅允许持锁访问 */
    private final Set<String> queuedTaskIds = new HashSet<>();

    /** 保护 tasks 以及 size/capacity 判断的独占锁 */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * 非空条件：消费者在此等待。
     *
     * <p>没有 notFull 条件，因为生产者采用「满即抛异常」的快速失败背压，
     * 不会在队列满时等待，避免占用请求线程。</p>
     */
    private final Condition notEmpty = lock.newCondition();

    /** 队列是否已关闭（停机标记）；关闭后不再接收新任务，消费者在队列取空后退出 */
    private volatile boolean closed;

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
     * @throws IllegalStateException  队列已关闭（服务正在停机）
     */
    public boolean offer(TaskEntity task) {
        Objects.requireNonNull(task, "任务不能为 null");
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("队列已关闭，不再接收新任务");
            }
            if (tasks.size() >= capacity) {
                throw new TaskQueueFullException(capacity, tasks.size());
            }
            // 二分查找插入点，保持列表按 TASK_ORDER 有序；插入为 O(n) 内存移动，容量 4096 级别可接受
            int index = Collections.binarySearch(tasks, task, TASK_ORDER);
            if (index < 0) {
                index = -index - 1;
            }
            tasks.add(index, task);
            queuedTaskIds.add(task.getTaskId());
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
     * @return 任务实体；队列已关闭且为空时返回 null，消费者据此退出
     * @throws InterruptedException 等待期间线程被中断（如优雅停机）
     */
    public TaskEntity take() throws InterruptedException {
        // 使用可中断加锁，保证停机时阻塞在锁上的消费者也能及时退出
        lock.lockInterruptibly();
        try {
            while (tasks.isEmpty()) {
                if (closed) {
                    // 关闭后不再等待，交给消费者线程退出
                    return null;
                }
                notEmpty.await();
            }
            TaskEntity task = tasks.remove(0);
            queuedTaskIds.remove(task.getTaskId());
            return task;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 指定任务是否仍在队列中（补偿扫描器据此避免重复投递）。
     */
    public boolean contains(String taskId) {
        if (taskId == null) {
            return false;
        }
        lock.lock();
        try {
            return queuedTaskIds.contains(taskId);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭队列：停止接收新任务，并唤醒所有阻塞在 take() 的消费者让它们退出。
     *
     * <p>已在队列中的任务不会被丢弃，仍可被 take() 取走，由停机流程先 drainAll() 回写。</p>
     */
    public void close() {
        lock.lock();
        try {
            closed = true;
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 队列是否已关闭。
     */
    public boolean isClosed() {
        return closed;
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
            queuedTaskIds.clear();
            return snapshot;
        } finally {
            lock.unlock();
        }
    }
}
