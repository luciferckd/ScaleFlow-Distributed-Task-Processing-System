const { redisClient } = require("../config/redis")

const STREAM_NAME = "scaleflow:tasks";

class QueueService {
    async addTask(taskId) {

        const messageId = await redisClient.xAdd(
            STREAM_NAME,
            "*",
            {
                taskId: String(taskId)
            }
        );

        console.log("Task added to Redis Stream:", {
            taskId,
            messageId
        });

        return messageId
    }

    async getQueueLength() {
        return await redisClient.xLen(STREAM_NAME);
    }
}

module.exports = new QueueService