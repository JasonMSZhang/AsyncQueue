package com.example.asyncqueue.exception;

/**
 * 系统可恢复异常：例如数据库连接异常、连接池耗尽、瞬时 IO 错误。
 *
 * <p>处理方式：按指数退避重新排期，但使用更长的间隔；同样不直接放回内存队列。</p>
 */
public class SystemRecoverableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SystemRecoverableException(String message) {
        super(message);
    }

    public SystemRecoverableException(String message, Throwable cause) {
        super(message, cause);
    }
}
