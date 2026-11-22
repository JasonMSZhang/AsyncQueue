package com.example.asyncqueue.exception;

/**
 * 服务正在停机，任务提交被拒绝。
 *
 * <p>上层接口应转换为 HTTP 503，让客户端稍后重试。</p>
 */
public class TaskSubmitClosedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TaskSubmitClosedException(String message) {
        super(message);
    }
}
