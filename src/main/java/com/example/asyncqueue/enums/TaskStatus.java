package com.example.asyncqueue.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

/**
 * 任务状态。
 *
 * <p>状态机：PENDING -> PROCESSING -> SUCCESS / FAILED / DEAD_LETTER</p>
 */
@Getter
@RequiredArgsConstructor
public enum TaskStatus {

    /** 排队中：已持久化，等待被调度 */
    PENDING("排队中"),

    /** 处理中：已被工作线程领取执行 */
    PROCESSING("处理中"),

    /** 成功：结果已落库 */
    SUCCESS("成功"),

    /** 失败：不可恢复或重试次数耗尽 */
    FAILED("失败"),

    /** 死信：需人工介入 */
    DEAD_LETTER("死信");

    private final String description;

    /** 是否为终态（终态任务不再参与调度与幂等复用） */
    public boolean isTerminal() {
        return this == SUCCESS || this == FAILED || this == DEAD_LETTER;
    }

    /** 未结束状态集合，用于按 businessKey 查询未完成任务 */
    public static List<TaskStatus> unfinished() {
        return List.of(PENDING, PROCESSING);
    }
}
