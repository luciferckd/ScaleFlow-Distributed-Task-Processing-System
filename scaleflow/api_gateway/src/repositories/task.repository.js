const pool = require('../config/db');

class TaskRepository {
  async create({ title, payload, userId, priority = 3 }) {
    const [result] = await pool.query(
      `INSERT INTO tasks (title, payload, user_id, priority, status) VALUES (?, ?, ?, ?, 'PENDING')`,
      [title, typeof payload === "object" ? JSON.stringify(payload) : payload, userId, priority]
    );
    return result.insertId;
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

