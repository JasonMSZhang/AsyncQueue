package com.example.asyncqueue.repository;

import com.example.asyncqueue.entity.TaskEntity;
import com.example.asyncqueue.enums.TaskStatus;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * 任务表数据访问。
 */
public interface TaskRepository extends JpaRepository<TaskEntity, String> {

    /**
     * 按业务键查询最近一条未结束任务，用于提交时的幂等复用。
     */
    Optional<TaskEntity> findFirstByBusinessKeyAndStatusInOrderByCreateTimeDesc(
            String businessKey, Collection<TaskStatus> statuses);

    /**
     * 查询到期可执行的待办任务：优先级高的先出，同优先级按创建时间先入先出。
     */
    List<TaskEntity> findByStatusAndNextExecuteTimeLessThanEqualOrderByPriorityDescCreateTimeAsc(
            TaskStatus status, LocalDateTime nextExecuteTime, Pageable pageable);

    /**
     * 按状态统计任务数量。
     */
    long countByStatus(TaskStatus status);

    /**
     * 启动加载专用：按 taskId 游标分批扫描待执行任务。
     *
     * <p>使用游标而不是 offset 分页：加载过程中工作线程会把任务从 PENDING 改成 PROCESSING，
     * offset 分页会因此漏读任务。</p>
     */
    List<TaskEntity> findByStatusAndNextExecuteTimeLessThanEqualAndTaskIdGreaterThanOrderByTaskIdAsc(
            TaskStatus status, LocalDateTime nextExecuteTime, String taskId, Pageable pageable);

    /**
     * 状态 CAS 更新：仅当当前状态符合预期时才更新，避免同一任务被重复消费。
     *
     * @return 受影响行数，0 表示状态已被其他线程/实例改变
     */
    @Modifying
    @Query("update TaskEntity t set t.status = :toStatus, t.updateTime = :updateTime "
            + "where t.taskId = :taskId and t.status = :fromStatus")
    int casStatus(@Param("taskId") String taskId,
                  @Param("fromStatus") TaskStatus fromStatus,
                  @Param("toStatus") TaskStatus toStatus,
                  @Param("updateTime") LocalDateTime updateTime);

    /**
     * 幂等查询的便捷方法：未结束 = PENDING / PROCESSING。
     */
    default Optional<TaskEntity> findUnfinishedByBusinessKey(String businessKey) {
        return findFirstByBusinessKeyAndStatusInOrderByCreateTimeDesc(businessKey, TaskStatus.unfinished());
    }

    /**
     * 待执行 PENDING 任务的便捷查询（限制单批数量，避免一次加载过多）。
     */
    default List<TaskEntity> findPendingToExecute(LocalDateTime now, int limit) {
        return findByStatusAndNextExecuteTimeLessThanEqualOrderByPriorityDescCreateTimeAsc(
                TaskStatus.PENDING, now, PageRequest.of(0, limit));
    }
}
