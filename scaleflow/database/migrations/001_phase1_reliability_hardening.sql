-- Phase 1: Reliability Hardening Migration
-- Adds lease locking columns to tasks and creates task_outbox table

ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS locked_by VARCHAR(100) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMP NULL DEFAULT NULL,
    ADD INDEX IF NOT EXISTS idx_tasks_status (status),
    ADD INDEX IF NOT EXISTS idx_tasks_lease (status, lease_expires_at);

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
