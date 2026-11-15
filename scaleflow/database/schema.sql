-- ScaleFlow Database Schema

CREATE DATABASE IF NOT EXISTS scaleflow;

USE scaleflow;

CREATE TABLE tasks (
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP
);
