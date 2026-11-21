
const taskRepository = require(
    "../repositories/task.repository"
);

const queueService = require("./queue.services");

const cacheService = require("./cache.services");

class TaskService {

    // Create Task with Transactional Outbox
    async createTask(data) {
        const { taskId, outboxId } = await taskRepository.createWithOutbox(data);

        let messageId = null;
        try {
            // Attempt immediate idempotent publish using outbox_id
            messageId = await queueService.addTask(taskId, outboxId);
            await taskRepository.markOutboxPublished(outboxId);
        } catch (error) {
            console.warn(
                `Direct Redis publish failed for task ${taskId} (outbox ${outboxId}). Background publisher will deliver it:`,
                error.message
            );
        }

        // Invalidate task list cache
        await cacheService.delete("tasks:all");

        return {
            taskId,
            outboxId,
            status: "PENDING",
            queueMessageId: messageId,
            message: "Task created and queued for processing"
        };
    }

    // Get All Tasks with Redis Cache
    async getTasks() {

        const cacheKey = "tasks:all";

        // Check Redis first
        const cachedTasks = await cacheService.get(cacheKey);

        if (cachedTasks !== null) {
            console.log("CACHE HIT - Get All Tasks");
            return { source: "redis", tasks: cachedTasks };
        }

        console.log("CACHE MISS - Get All Tasks");

        // Fetch from MySQL
        const tasks = await taskRepository.findAll();

        // Store in Redis for 60 seconds
        await cacheService.set(cacheKey, tasks, 60);

        return { source: "mysql", tasks };
    }

    // Get Single Task with Redis Cache
    async getTask(id) {

        const cacheKey = `task:${id}`;

        // Check Redis
        const cachedTask = await cacheService.get(cacheKey);

        if (cachedTask !== null) {
            console.log("CACHE HIT - Task:", id);
            return { source: "redis", task: cachedTask };
        }

        console.log("CACHE MISS - Task:", id);

        // Fetch from MySQL
        const task = await taskRepository.findById(id);

        if (!task) {
            return null;
        }

        // Save in Redis
        await cacheService.set(cacheKey, task, 60);

        return { source: "mysql", task };
    }

    // Update Task Status
    async updateTaskStatus(id, status) {

        const allowedStatuses = [
            "PENDING",
            "PROCESSING",
            "COMPLETED",
            "FAILED"
        ];

        if (!allowedStatuses.includes(status)) {
            throw new Error("Invalid task status");
        }

        const result = await taskRepository.updateStatus(id, status);

        // Invalidate caches only after successful update
        if (result) {
            await cacheService.delete(`task:${id}`);
            await cacheService.delete("tasks:all");
        }

        return result;
    }
}

module.exports = new TaskService();
