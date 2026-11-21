const pool = require('../config/db');
const { redisClient } = require('../config/redis');

class HealthService {

    getLiveness() {
        return {
            status: 'UP',
            service: 'scaleflow-api',
            timestamp: new Date().toISOString(),
            uptimeSeconds: Math.floor(process.uptime())
        };
    }

    async getReadiness() {
        const components = {};
        let isHealthy = true;

        // 1. Check MySQL
        const dbStart = Date.now();
        try {
            await pool.query('SELECT 1');
            components.database = {
                status: 'UP',
                latencyMs: Date.now() - dbStart
            };
        } catch (error) {
            isHealthy = false;
            components.database = {
                status: 'DOWN',
                error: error.message
            };
        }

        // 2. Check Redis
        const redisStart = Date.now();
        try {
            const pong = await redisClient.ping();
            components.redis = {
                status: pong === 'PONG' ? 'UP' : 'DEGRADED',
                latencyMs: Date.now() - redisStart
            };
        } catch (error) {
            isHealthy = false;
            components.redis = {
                status: 'DOWN',
                error: error.message
            };
        }

        return {
            status: isHealthy ? 'UP' : 'DOWN',
            service: 'scaleflow-api',
            timestamp: new Date().toISOString(),
            uptimeSeconds: Math.floor(process.uptime()),
            components
        };
    }

    async getSystemMetrics() {
        const metrics = {
            timestamp: new Date().toISOString(),
            queues: {},
            tasks: {},
            outbox: {}
        };

        // Queue metrics from Redis
        try {
            if (redisClient.isOpen) {
                metrics.queues.activeTasksQueueLength = await redisClient.xLen('scaleflow:tasks');
                metrics.queues.deadLetterQueueLength = await redisClient.xLen('scaleflow:tasks:dead');
            }
        } catch (error) {
            metrics.queues.error = error.message;
        }

        // Database metrics
        try {
            // Task status breakdown
            const [taskRows] = await pool.query(
                `SELECT status, COUNT(*) AS count FROM tasks GROUP BY status`
            );
            taskRows.forEach(row => {
                metrics.tasks[row.status.toLowerCase()] = row.count;
            });

            // Pending outbox count
            const [outboxRows] = await pool.query(
                `SELECT COUNT(*) AS pendingOutbox FROM task_outbox WHERE published = FALSE`
            );
            metrics.outbox.unpublishedEvents = outboxRows[0]?.pendingOutbox || 0;

            // Expired lease / orphaned count
            const [orphanRows] = await pool.query(
                `SELECT COUNT(*) AS orphaned FROM tasks WHERE status = 'PROCESSING' AND lease_expires_at < NOW()`
            );
            metrics.tasks.orphanedExpiredLeases = orphanRows[0]?.orphaned || 0;
        } catch (error) {
            metrics.databaseError = error.message;
        }

        return metrics;
    }
}

module.exports = new HealthService();
