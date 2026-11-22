package com.example.asyncqueue.repository;

import com.example.asyncqueue.entity.TaskEntity;
import com.example.asyncqueue.enums.TaskStatus;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

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
