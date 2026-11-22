package com.example.asyncqueue.repository;

import com.example.asyncqueue.entity.TaskResultEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * 任务结果表数据访问。
 */
public interface TaskResultRepository extends JpaRepository<TaskResultEntity, String> {

    Optional<TaskResultEntity> findByTaskId(String taskId);

    boolean existsByTaskId(String taskId);
}
