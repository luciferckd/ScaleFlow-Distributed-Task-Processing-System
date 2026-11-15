const taskService = require("../services/tasks.services");

exports.createTask = async (req, res) => {
    try {
        const { title, payload, userId, priority } = req.body;

        if (!title || !payload || !userId) {
            return res.status(400).json({
                message: "Required fields are missing"
            });
        }

        const task = await taskService.createTask({
            title,
            payload,
            userId,
            priority
        });

        return res.status(201).json({
            message: "Task created successfully",
            data: task
        });

    } catch (error) {
        return res.status(500).json({
            message: error.message
        });
    }
};

exports.getTasks = async (req, res) => {
    try {
        const tasks = await taskService.getTasks();

        return res.status(200).json({
            data: tasks
        });

    } catch (error) {
        return res.status(500).json({
            message: error.message
        });
    }
};

exports.getTask = async (req, res) => {
    try {
        const task = await taskService.getTask(req.params.id);

        if (!task) {
            return res.status(404).json({
                message: "Task not found"
            });
        }

        return res.status(200).json({
            data: task
        });

    } catch (error) {
        return res.status(500).json({
            message: error.message
        });
    }
};