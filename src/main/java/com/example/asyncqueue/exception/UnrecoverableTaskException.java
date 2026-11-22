package com.example.asyncqueue.exception;

/**
 * 不可恢复异常：相同输入重试必然同样失败，例如参数 JSON 无法解析、实验ID不存在。
 *
 * <p>处理方式：不重试，直接进入死信（DEAD_LETTER）。</p>
 */
public class UnrecoverableTaskException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public UnrecoverableTaskException(String message) {
        super(message);
    }

    public UnrecoverableTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
