-- =====================================================================
-- 异步任务队列表结构（H2 文件模式）
-- 每次启动都会执行本脚本，因此必须保持幂等（IF NOT EXISTS）
-- =====================================================================

-- ---------------------------------------------------------------------
-- 任务主表：只保存调度所需信息，结果大字段拆分到 t_task_result
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS t_task
(
    task_id           VARCHAR(32)  NOT NULL,
    business_key      VARCHAR(32)  NOT NULL,
    priority          INT          NOT NULL DEFAULT 5,
    task_param_json   CLOB         NOT NULL,
    status            VARCHAR(16)  NOT NULL,
    retry_count       INT          NOT NULL DEFAULT 0,
    max_retry         INT          NOT NULL DEFAULT 3,
    next_execute_time TIMESTAMP    NOT NULL,
    fail_reason       VARCHAR(512),
    create_time       TIMESTAMP    NOT NULL,
    update_time       TIMESTAMP    NOT NULL,
    CONSTRAINT pk_t_task PRIMARY KEY (task_id),
    CONSTRAINT ck_t_task_status CHECK (status IN ('PENDING', 'PROCESSING', 'SUCCESS', 'FAILED', 'DEAD_LETTER'))
);

-- 待执行扫描：WHERE status = ? AND next_execute_time <= ? ORDER BY priority DESC, create_time ASC
CREATE INDEX IF NOT EXISTS idx_t_task_status_next
    ON t_task (status, next_execute_time, priority);

-- 幂等查询：WHERE business_key = ? AND status IN (...)
CREATE INDEX IF NOT EXISTS idx_t_task_business_key
    ON t_task (business_key, status, create_time);

-- ---------------------------------------------------------------------
-- 结果表：与任务表严格分离，一对一（task_id 唯一）
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS t_task_result
(
    id            VARCHAR(32) NOT NULL,
    task_id       VARCHAR(32) NOT NULL,
    result_json   CLOB        NOT NULL,
    complete_time TIMESTAMP   NOT NULL,
    CONSTRAINT pk_t_task_result PRIMARY KEY (id),
    CONSTRAINT uk_t_task_result_task_id UNIQUE (task_id),
    CONSTRAINT fk_t_task_result_task FOREIGN KEY (task_id) REFERENCES t_task (task_id)
);
