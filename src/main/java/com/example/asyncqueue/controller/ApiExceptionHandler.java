package com.example.asyncqueue.controller;

import com.example.asyncqueue.exception.TaskSubmitClosedException;
import com.example.asyncqueue.queue.TaskQueueFullException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 接口异常统一转换。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    /**
     * 停机中：拒绝新任务。
     */
    @ExceptionHandler(TaskSubmitClosedException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Map<String, String> handleSubmitClosed(TaskSubmitClosedException e) {
        return body(e.getMessage());
    }

    /**
     * 队列已满：任务已落库，客户端稍后用相同参数重试即可拿到同一个 taskId。
     */
    @ExceptionHandler(TaskQueueFullException.class)
    @ResponseStatus(HttpStatus.TOO_MANY_REQUESTS)
    public Map<String, String> handleQueueFull(TaskQueueFullException e) {
        return body("任务队列繁忙，任务已持久化，请稍后使用相同参数重试");
    }

    /**
     * 参数校验失败。
     */
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleIllegalArgument(IllegalArgumentException e) {
        return body(e.getMessage());
    }

    /**
     * 缺少必填参数。
     */
    @ExceptionHandler(MissingServletRequestParameterException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleMissingParameter(MissingServletRequestParameterException e) {
        return body("缺少必填参数: " + e.getParameterName());
    }

    private Map<String, String> body(String message) {
        Map<String, String> body = new LinkedHashMap<>();
        body.put("message", message);
        return body;
    }
}
