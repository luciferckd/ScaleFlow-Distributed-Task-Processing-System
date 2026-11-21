require("dotenv").config();
const app = require("./app");
const { connectRedis } = require("./config/redis");

const PORT = process.env.PORT || 5000;

async function bootstrap() {
    try {
        await connectRedis();
        app.listen(PORT, () => {
            console.log(`ScaleFlow API running on port ${PORT}`);
        });
    } catch (error) {
        console.error("Failed to start API Gateway:", error);
        process.exit(1);
    }
}

bootstrap();

