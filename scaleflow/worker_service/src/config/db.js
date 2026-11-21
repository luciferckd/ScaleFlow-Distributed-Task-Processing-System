const mysql = require('mysql2/promise');
require('dotenv').config();

const pool = mysql.createPool({
  host: process.env.DB_HOST || 'localhost',
  port: Number(process.env.DB_PORT) || 3306,
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'scaleflow',
  waitForConnections: true,
  connectionLimit: Number(process.env.DB_CONNECTION_LIMIT) || 10,
  queueLimit: 0,
  // Database connection timeout handling
  connectTimeout: Number(process.env.DB_CONNECT_TIMEOUT) || 5000, // 5s connection timeout
  enableKeepAlive: true,                                         // TCP keep-alive to prevent dropped connections
  keepAliveInitialDelay: 10000,                                  // Initial keep-alive delay (10s)
  idleTimeout: 30000,                                            // Close idle connections after 30s
  maxIdle: 5
});

module.exports = pool;

