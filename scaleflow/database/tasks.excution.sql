CREATE TABLE task_executions (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    task_id BIGINT NOT NULL,
    attempt_number INT NOT NULL,
    worker_id VARCHAR(100),
    status ENUM(
        'PROCESSING',
        'COMPLETED',
        'FAILED'
    ) NOT NULL,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    UNIQUE KEY uq_task_attempt (task_id, attempt_number),
    FOREIGN KEY (task_id) REFERENCES tasks(id)
);