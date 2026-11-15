const { createClient } = require('redis');
require('dotenv').config();

const redisClient = createClient({
    socket: {
        host: process.env.REDIS_HOST || "localhost",
        port: Number(process.env.REDIS_PORT || 6379)
    }
});

redisClient.on("error", (err) => {
    console.error("Redis Error:", err.message);
});

async function connectRedis() {
    if (!redisClient.isOpen) {
        await redisClient.connect();
    }

    console.log("Redis Connected Successfully");
}

module.exports = {
    redisClient,
    connectRedis
};
