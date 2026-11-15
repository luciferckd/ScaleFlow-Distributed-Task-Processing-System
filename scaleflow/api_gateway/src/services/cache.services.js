const { redisClient } = require("../config/redis");

class CacheService {
    async get(key) {
        const data = await redisClient.get(key);

        if (!data) {
            return null;
        }

        return JSON.parse(data);
    }

    async set(key, value, ttl = 60) {
        await redisClient.set(
            key,
            JSON.stringify(value),
            {
                EX: ttl
            }
        );
    }

    async delete(key) {
        await redisClient.del(key);
    }

}

module.exports = new CacheService();