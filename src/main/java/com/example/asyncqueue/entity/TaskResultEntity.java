package com.example.asyncqueue.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

/**
 * 任务结果表实体：与 {@link TaskEntity} 一对一（task_id 唯一）。
 *
 * <p>独立成表的原因：结果只写一次、只读多次，CLOB 大字段不参与任务状态更新，
 * 避免主表写放大与扫描时把结果读进内存。</p>
 */
@Entity
@Table(name = "t_task_result")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskResultEntity {

    /** 结果记录ID，UUID（32 位无横线） */
    @Id
    @Column(name = "id", length = 32, nullable = false, updatable = false)
    private String id;

    /** 关联的任务ID，一对一，唯一 */
    @Column(name = "task_id", length = 32, nullable = false, unique = true)
    private String taskId;

    /** AB 分桶结果 JSON：每个实验的桶号与实验组/对照组 */
    @Lob
    @Column(name = "result_json", nullable = false)
    private String resultJson;

    /** 结果完成时间 */
    @Column(name = "complete_time", nullable = false)
    private LocalDateTime completeTime;

    @PrePersist
    void prePersist() {
        if (completeTime == null) {
            completeTime = LocalDateTime.now();
        }
    }
}
