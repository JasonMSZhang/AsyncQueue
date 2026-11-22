package com.example.asyncqueue.queue;

/**
 * 队列已满异常：作为内存队列的背压信号。
 *
 * <p>任务在入队前已经持久化到 H2，抛出该异常只代表「暂时进不了内存队列」，
 * 任务本身不会丢失，由补偿扫描器后续重新投递。</p>
 */
public class TaskQueueFullException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** 队列容量 */
    private final int capacity;

    /** 触发异常时的队列元素数量 */
    private final int size;

    public TaskQueueFullException(int capacity, int size) {
        super("任务队列已满，触发背压: capacity=" + capacity + ", size=" + size);
        this.capacity = capacity;
        this.size = size;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getSize() {
        return size;
    }
}
