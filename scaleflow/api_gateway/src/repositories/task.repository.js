const pool = require('../config/db');

class TaskRepository {
  async create({ title, payload, userId, priority = 3 }) {
    const [result] = await pool.query(
      `INSERT INTO tasks (title, payload, user_id, priority, status) VALUES (?, ?, ?, ?, 'PENDING')`,
      [title, typeof payload === "object" ? JSON.stringify(payload) : payload, userId, priority]
    );
    return result.insertId;
  }

  async createWithOutbox({ title, payload, userId, priority = 3 }) {
    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();

      const [taskResult] = await connection.query(
        `INSERT INTO tasks (title, payload, user_id, priority, status) VALUES (?, ?, ?, ?, 'PENDING')`,
        [title, typeof payload === "object" ? JSON.stringify(payload) : payload, userId, priority]
      );
      const taskId = taskResult.insertId;

      const outboxPayload = {
        taskId: String(taskId),
        title,
        userId: String(userId),
        priority: String(priority)
      };

      const [outboxResult] = await connection.query(
        `INSERT INTO task_outbox (task_id, event_type, payload) VALUES (?, 'TASK_CREATED', ?)`,
        [taskId, JSON.stringify(outboxPayload)]
      );

      await connection.commit();

      return {
        taskId,
        outboxId: outboxResult.insertId
      };
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  }

  async markOutboxPublished(outboxId) {
    await pool.query(
      `UPDATE task_outbox SET published = TRUE, published_at = NOW() WHERE id = ?`,
      [outboxId]
    );
  }

  async findAll() {
    const [rows] = await pool.query(`SELECT * FROM tasks ORDER BY created_at DESC`);
    return rows;
  }

  async findById(id) {
    const [rows] = await pool.query(`SELECT * FROM tasks WHERE id = ?`, [id]);
    return rows[0] || null;
  }

  async updateStatus(id, status) {
    await pool.query(`UPDATE tasks SET status = ? WHERE id = ?`, [status, id]);
    return this.findById(id);
  }
}

module.exports = new TaskRepository();

