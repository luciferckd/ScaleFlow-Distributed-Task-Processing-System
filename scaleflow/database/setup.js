const fs = require('fs');
const path = require('path');

// Resolve modules from api_gateway node_modules
const gatewayPath = path.join(__dirname, '../api_gateway');
const mysql = require(path.join(gatewayPath, 'node_modules/mysql2/promise'));
require(path.join(gatewayPath, 'node_modules/dotenv')).config({ path: path.join(gatewayPath, '.env') });

async function initDB() {
  const host = process.env.DB_HOST || 'localhost';
  const port = Number(process.env.DB_PORT) || 3306;
  const user = process.env.DB_USER || 'root';
  const password = process.env.DB_PASSWORD || '';

  console.log(`Connecting to MySQL server at ${host}:${port} as user '${user}'...`);

  let connection;
  try {
    connection = await mysql.createConnection({
      host,
      port,
      user,
      password,
      multipleStatements: true,
    });

    const schemaPath = path.join(__dirname, 'schema.sql');
    const sql = fs.readFileSync(schemaPath, 'utf8');

    console.log('Executing schema.sql...');
    await connection.query(sql);

    console.log('Database `scaleflow` and `tasks` table initialized successfully!');
  } catch (error) {
    console.error('Database initialization failed:', error.message);
    if (error.code === 'ER_ACCESS_DENIED_ERROR') {
      console.error('\nNote: Please set your MySQL root password in api_gateway/.env (DB_PASSWORD=your_password).');
    }
    process.exit(1);
  } finally {
    if (connection) {
      await connection.end();
    }
  }
}

initDB();
