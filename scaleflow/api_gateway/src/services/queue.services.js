const { redisClient } = require("../config/redis");

const STREAM_NAME = "scaleflow:tasks";

// Idempotent Outbox Publishing Lua Script
// Ensures only one Redis message is created per outbox_id even during retries or concurrent publishers
const OUTBOX_PUBLISH_SCRIPT = `
local existing_id = redis.call('GET', KEYS[1])

if existing_id then
    return existing_id
end

local message_id = redis.call(
    'XADD',
    KEYS[2],
    '*',
    unpack(ARGV)
)

redis.call('SET', KEYS[1], message_id, 'EX', 604800)

return message_id
`;

class QueueService {
    async addTask(taskId, outboxId = null) {
        if (!outboxId) {
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

            return messageId;
        }

        const markerKey = `scaleflow:outbox:published:${outboxId}`;
        const fields = [
            "taskId", String(taskId),
            "outbox_id", String(outboxId)
        ];

        // Idempotent publish via Lua script
        const messageId = await redisClient.eval(
            OUTBOX_PUBLISH_SCRIPT,
            {
                keys: [markerKey, STREAM_NAME],
                arguments: fields
            }
        );

        console.log("Task added to Redis Stream (Idempotent Outbox):", {
            taskId,
            outboxId,
            messageId
        });

        return messageId;
    }

    async getQueueLength() {
        return await redisClient.xLen(STREAM_NAME);
    }
}

module.exports = new QueueService();
