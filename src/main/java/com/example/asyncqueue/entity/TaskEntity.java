package com.example.asyncqueue.entity;

import com.example.asyncqueue.enums.TaskStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

/**
 * 任务主表实体：只保存调度信息，业务结果存 {@link TaskResultEntity}。
 */
@Entity
@Table(name = "t_task")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskEntity {

    /** 任务ID，UUID（32 位无横线） */
    @Id
    @Column(name = "task_id", length = 32, nullable = false, updatable = false)
    private String taskId;

    /** 幂等业务键：md5(uid + 排序后的实验ID列表) */
    @Column(name = "business_key", length = 32, nullable = false)
    private String businessKey;

    /** 任务优先级，0~9，数值越大越先执行 */
    @Builder.Default
    @Column(name = "priority", nullable = false)
    private Integer priority = 5;

    /** AB 任务入参 JSON：uid、expId 列表等 */
    @Lob
    @Column(name = "task_param_json", nullable = false)
    private String taskParamJson;

    /** 任务状态 */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 16, nullable = false)
    private TaskStatus status;

    /** 已重试次数（不含首次执行） */
    @Builder.Default
    @Column(name = "retry_count", nullable = false)
    private Integer retryCount = 0;

    /** 最大重试次数 */
    @Builder.Default
    @Column(name = "max_retry", nullable = false)
    private Integer maxRetry = 3;

    /** 下次可执行时间：提交时等于创建时间，重试时体现退避间隔 */
    @Column(name = "next_execute_time", nullable = false)
    private LocalDateTime nextExecuteTime;

    /** 最近一次失败原因（截断摘要，完整堆栈入日志） */
    @Column(name = "fail_reason", length = 512)
    private String failReason;

    @Column(name = "create_time", nullable = false, updatable = false)
    private LocalDateTime createTime;

    @Column(name = "update_time", nullable = false)
    private LocalDateTime updateTime;

    @PrePersist
    void prePersist() {
        LocalDateTime now = LocalDateTime.now();
        if (createTime == null) {
            createTime = now;
        }
        updateTime = now;
        if (nextExecuteTime == null) {
            nextExecuteTime = now;
        }
        if (status == null) {
            status = TaskStatus.PENDING;
        }
    }

    @PreUpdate
    void preUpdate() {
        updateTime = LocalDateTime.now();
    }
}
