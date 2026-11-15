const router = require("express").Router();

const controller = require(
    "../controllers/task.controller"
);

router.post("/", controller.createTask);
router.get("/", controller.getTasks);
router.get("/:id", controller.getTask);

module.exports = router;