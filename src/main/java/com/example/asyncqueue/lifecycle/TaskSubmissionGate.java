package com.example.asyncqueue.lifecycle;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 任务提交全局开关。
 *
 * <p>停机第一阶段置为关闭：所有新提交直接拒绝，避免停机过程中还有任务进入内存队列。</p>
 */
@Component
public class TaskSubmissionGate {

    private final AtomicBoolean accepting = new AtomicBoolean(true);

    /**
     * 当前是否接收新任务。
     */
    public boolean isAccepting() {
        return accepting.get();
    }

    /**
     * 停止接收新任务（停机专用，单向操作）。
     */
    public void stopAccepting() {
        accepting.set(false);
    }
}
