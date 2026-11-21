require("dotenv").config()

const express = require('express');
const routes = require('./routes');
const errorHandler = require('./middlewares/errorHandler');
const { connectRedis } = require('./config/redis');

const app = express();


app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Health / Base Routes
app.use('/', routes);

// API Routes
app.use(
    "/api/tasks",
    require("./routes/tasks.routes")
);

// Centralized Error Handling
app.use(errorHandler);

// Start server if run directly (e.g. node src/app.js)
async function startServer() {
    try {
        await connectRedis();
        const PORT = process.env.PORT || 5000;

        app.listen(PORT, () => {
            console.log(`ScaleFlow API running on port http://localhost:${PORT}`);
        });
    } catch (error) {
        console.error(`Redis connection error:`, error);
        process.exit(1);
    }
}

if (require.main === module) {
    startServer();
}

module.exports = app;
