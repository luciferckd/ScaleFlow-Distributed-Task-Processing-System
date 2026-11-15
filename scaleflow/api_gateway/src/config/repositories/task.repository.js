const db = require("../db");

class TaskRepository {

    async create(task) {
        const [result] = await db.execute(
            `INSERT INTO tasks
            (user_id, title, payload, priority)
            VALUES (?, ?, ?, ?)`,
            [
                task.userId,
                task.title,
                JSON.stringify(task.payload),
                task.priority || 3
            ]
        );

        return result.insertId;
    }

    async findAll() {
        const [rows] = await db.execute(
            "SELECT * FROM tasks ORDER BY id DESC"
        );

        return rows;
    }

    async findById(id) {
        const [rows] = await db.execute(
            "SELECT * FROM tasks WHERE id = ?",
            [id]
        );

        return rows[0] || null;
    }

    async updateStatus(id, status) {
        const [result] = await db.execute(
            "UPDATE tasks SET status = ? WHERE id = ?",
            [status, id]
        );

        return result.affectedRows;
    }
}

module.exports = new TaskRepository();