package com.example.asyncqueue.exception;

/**
 * 业务可重试异常：例如业务临时性报错、可预期的短暂失败。
 *
 * <p>处理方式：未超过 maxRetry 时按指数退避重新排期（状态回到 PENDING，写入 nextExecuteTime），
 * 不直接放回内存队列。</p>
 */
public class BusinessRetryableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public BusinessRetryableException(String message) {
        super(message);
    }

    public BusinessRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
