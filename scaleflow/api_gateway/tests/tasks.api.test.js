const { describe, it, after, beforeEach, mock } = require('node:test');
const assert = require('node:assert');
const pool = require('../src/config/db');

// Test Suite for TaskService, TaskController & API Endpoints
const taskService = require('../src/services/tasks.services');
const taskRepository = require('../src/repositories/task.repository');
const queueService = require('../src/services/queue.services');
const cacheService = require('../src/services/cache.services');
const taskController = require('../src/controllers/task.controller');

describe('ScaleFlow API Gateway - Task Unit & API Tests', () => {
    after(async () => {
        try {
            await pool.end();
        } catch (_) {}
    });

    describe('1. Task Creation Validation (Controller)', () => {
        it('should return 400 when required fields are missing', async () => {
            let statusCode = null;
            let responseBody = null;

            const req = {
                body: { title: 'Missing payload and userId' }
            };
            const res = {
                status(code) {
                    statusCode = code;
                    return this;
                },
                json(data) {
                    responseBody = data;
                    return this;
                }
            };

            await taskController.createTask(req, res);

            assert.strictEqual(statusCode, 400);
            assert.strictEqual(responseBody.message, 'Required fields are missing');
        });

        it('should return 201 and task data when all required fields are provided', async () => {
            let statusCode = null;
            let responseBody = null;

            const req = {
                body: {
                    title: 'Send Invoice',
                    payload: { invoiceId: 101, amount: 250 },
                    userId: 42,
                    priority: 2
                }
            };
            const res = {
                status(code) {
                    statusCode = code;
                    return this;
                },
                json(data) {
                    responseBody = data;
                    return this;
                }
            };

            // Mock service layer
            mock.method(taskRepository, 'createWithOutbox', async () => {
                return { taskId: 501, outboxId: 901 };
            });
            mock.method(queueService, 'addTask', async () => '1700000000000-0');
            mock.method(taskRepository, 'markOutboxPublished', async () => {});
            mock.method(cacheService, 'delete', async () => {});

            await taskController.createTask(req, res);

            assert.strictEqual(statusCode, 201);
            assert.strictEqual(responseBody.message, 'Task created successfully');
            assert.strictEqual(responseBody.data.taskId, 501);
            assert.strictEqual(responseBody.data.outboxId, 901);
            assert.strictEqual(responseBody.data.status, 'PENDING');
            assert.strictEqual(responseBody.data.queueMessageId, '1700000000000-0');
        });
    });

    describe('2. TaskService - Transactional Outbox & Graceful Fallback', () => {
        it('should create task and attempt idempotent outbox queue publish', async () => {
            let outboxMarked = false;

            mock.method(taskRepository, 'createWithOutbox', async () => ({
                taskId: 601,
                outboxId: 902
            }));
            mock.method(queueService, 'addTask', async (taskId, outboxId) => {
                assert.strictEqual(taskId, 601);
                assert.strictEqual(outboxId, 902);
                return '1700000000000-1';
            });
            mock.method(taskRepository, 'markOutboxPublished', async (outboxId) => {
                if (outboxId === 902) outboxMarked = true;
            });
            mock.method(cacheService, 'delete', async () => {});

            const result = await taskService.createTask({
                title: 'Process Video',
                payload: { videoId: 'abc' },
                userId: 1
            });

            assert.strictEqual(result.taskId, 601);
            assert.strictEqual(result.outboxId, 902);
            assert.strictEqual(result.status, 'PENDING');
            assert.strictEqual(result.queueMessageId, '1700000000000-1');
            assert.strictEqual(outboxMarked, true);
        });

        it('should gracefully handle Redis downtime without failing task creation', async () => {
            mock.method(taskRepository, 'createWithOutbox', async () => ({
                taskId: 602,
                outboxId: 903
            }));
            // Simulate Redis offline during immediate publish
            mock.method(queueService, 'addTask', async () => {
                throw new Error('Connection to Redis refused');
            });
            mock.method(cacheService, 'delete', async () => {});

            // Should NOT throw; task is safely persisted in MySQL outbox for background delivery
            const result = await taskService.createTask({
                title: 'Send Push Notification',
                payload: { text: 'Hello' },
                userId: 2
            });

            assert.strictEqual(result.taskId, 602);
            assert.strictEqual(result.outboxId, 903);
            assert.strictEqual(result.status, 'PENDING');
            assert.strictEqual(result.queueMessageId, null);
        });
    });

    describe('3. Task Retrieval & Caching', () => {
        it('should return cached tasks on Redis cache hit', async () => {
            const cachedList = [
                { id: 1, title: 'Task 1', status: 'COMPLETED' },
                { id: 2, title: 'Task 2', status: 'PENDING' }
            ];

            mock.method(cacheService, 'get', async (key) => {
                if (key === 'tasks:all') return cachedList;
                return null;
            });

            const result = await taskService.getTasks();

            assert.strictEqual(result.source, 'redis');
            assert.deepStrictEqual(result.tasks, cachedList);
        });

        it('should fetch from MySQL and backfill Redis on cache miss', async () => {
            const dbList = [
                { id: 3, title: 'Task 3', status: 'PENDING' }
            ];
            let cachedKey = null;
            let cachedVal = null;

            mock.method(cacheService, 'get', async () => null);
            mock.method(taskRepository, 'findAll', async () => dbList);
            mock.method(cacheService, 'set', async (key, val) => {
                cachedKey = key;
                cachedVal = val;
            });

            const result = await taskService.getTasks();

            assert.strictEqual(result.source, 'mysql');
            assert.deepStrictEqual(result.tasks, dbList);
            assert.strictEqual(cachedKey, 'tasks:all');
            assert.deepStrictEqual(cachedVal, dbList);
        });

        it('should return 404 in controller when task is not found', async () => {
            let statusCode = null;
            let responseBody = null;

            const req = { params: { id: 9999 } };
            const res = {
                status(code) {
                    statusCode = code;
                    return this;
                },
                json(data) {
                    responseBody = data;
                    return this;
                }
            };

            mock.method(cacheService, 'get', async () => null);
            mock.method(taskRepository, 'findById', async () => null);

            await taskController.getTask(req, res);

            assert.strictEqual(statusCode, 404);
            assert.strictEqual(responseBody.message, 'Task not found');
        });
    });

    describe('4. Task Status Transitions', () => {
        it('should allow valid status updates and invalidate cache', async () => {
            let invalidated = [];

            mock.method(taskRepository, 'updateStatus', async (id, status) => ({
                id,
                status
            }));
            mock.method(cacheService, 'delete', async (key) => {
                invalidated.push(key);
            });

            const result = await taskService.updateTaskStatus(10, 'COMPLETED');

            assert.strictEqual(result.status, 'COMPLETED');
            assert.ok(invalidated.includes('task:10'));
            assert.ok(invalidated.includes('tasks:all'));
        });

        it('should throw an error on invalid status values', async () => {
            await assert.rejects(
                async () => {
                    await taskService.updateTaskStatus(10, 'UNKNOWN_STATUS');
                },
                {
                    name: 'Error',
                    message: 'Invalid task status'
                }
            );
        });
    });

    describe('5. Structured Logger', () => {
        const logger = require('../src/utils/logger');

        it('should format logs with timestamp and metadata', () => {
            let loggedMessage = '';
            const originalLog = console.log;
            console.log = (msg) => { loggedMessage = msg; };

            try {
                logger.info('Task created successfully', {
                    event: 'TASK_CREATED',
                    taskId: 777
                });

                assert.ok(loggedMessage.includes('INFO'));
                assert.ok(loggedMessage.includes('Task created successfully'));
                assert.ok(loggedMessage.includes('777'));
            } finally {
                console.log = originalLog;
            }
        });
    });

    describe('6. Health Checks & Monitoring Service', () => {
        const healthService = require('../src/services/health.services');
        const pool = require('../src/config/db');
        const { redisClient } = require('../src/config/redis');

        it('should return UP for liveness check', () => {
            const liveness = healthService.getLiveness();
            assert.strictEqual(liveness.status, 'UP');
            assert.strictEqual(liveness.service, 'scaleflow-api');
            assert.ok(typeof liveness.uptimeSeconds === 'number');
        });

        it('should return UP readiness when MySQL and Redis are healthy', async () => {
            mock.method(pool, 'query', async () => [[{ 1: 1 }]]);
            mock.method(redisClient, 'ping', async () => 'PONG');

            const readiness = await healthService.getReadiness();
            assert.strictEqual(readiness.status, 'UP');
            assert.strictEqual(readiness.components.database.status, 'UP');
            assert.strictEqual(readiness.components.redis.status, 'UP');
        });

        it('should return DOWN readiness when database fails', async () => {
            mock.method(pool, 'query', async () => {
                throw new Error('Database connection failed');
            });
            mock.method(redisClient, 'ping', async () => 'PONG');

            const readiness = await healthService.getReadiness();
            assert.strictEqual(readiness.status, 'DOWN');
            assert.strictEqual(readiness.components.database.status, 'DOWN');
        });
    });
});
