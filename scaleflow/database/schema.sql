-- ScaleFlow Database Schema

CREATE DATABASE IF NOT EXISTS scaleflow;

USE scaleflow;

CREATE TABLE IF NOT EXISTS tasks (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    title VARCHAR(255) NOT NULL,
    payload JSON NOT NULL,
    status ENUM(
        'PENDING',
        'PROCESSING',
        'COMPLETED',
        'FAILED'
    ) NOT NULL DEFAULT 'PENDING',
    priority INT DEFAULT 3,
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    locked_by VARCHAR(100) DEFAULT NULL,
    lease_expires_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_tasks_status (status),
    INDEX idx_tasks_lease (status, lease_expires_at)
);

CREATE TABLE IF NOT EXISTS task_executions (
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
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS task_results (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    task_id BIGINT NOT NULL UNIQUE,
    result_json JSON,
    s3_key VARCHAR(1024),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS task_outbox (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    task_id BIGINT NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload JSON NOT NULL,
    published BOOLEAN DEFAULT FALSE,
    published_at TIMESTAMP NULL DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_outbox_published (published, id),
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
);
