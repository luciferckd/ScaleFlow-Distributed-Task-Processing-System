# ScaleFlow

## Project Structure

```
scaleflow/
├── api_gateway/          # Express API Gateway (Routing, Auth, DB/Redis Client)
│   ├── src/
│   │   ├── config/       # Database & Redis configurations
│   │   ├── controllers/  # Request handlers
│   │   ├── middlewares/  # Error & authentication middlewares
│   │   ├── routes/       # API route definitions
│   │   ├── services/     # Core business & gateway logic
│   │   ├── utils/        # Utility helpers
│   │   ├── app.js        # Express app initialization
│   │   └── server.js     # Server entrypoint
│   ├── .env.example
│   └── package.json
├── worker_service/       # Background Worker Service (Async jobs & queues)
│   ├── src/
│   │   ├── config/       # Worker DB & Redis configs
│   │   ├── jobs/         # Job definitions
│   │   ├── workers/      # Queue consumers/workers
│   │   ├── utils/        # Utility helpers
│   │   └── index.js      # Worker entrypoint
│   ├── .env.example
│   └── package.json
├── database/             # Database Schemas & Migrations
│   ├── migrations/       # SQL migration scripts
│   ├── seeds/            # Initial seed data
│   └── schema.sql        # Database schema definitions
└── tests/                # Automated Tests
    ├── unit/             # Unit tests
    └── integration/      # Integration tests
```
