const express = require('express');
const router = express.Router();
const healthService = require('../services/health.services');

// 1. Lightweight Liveness Probe (for Kubernetes / Docker liveness)
router.get('/health/live', (req, res) => {
    const liveness = healthService.getLiveness();
    res.status(200).json(liveness);
});

// 2. Comprehensive Readiness & Health Check (verifies DB & Redis)
router.get(['/health', '/health/ready'], async (req, res) => {
    try {
        const readiness = await healthService.getReadiness();
        const statusCode = readiness.status === 'UP' ? 200 : 503;
        res.status(statusCode).json(readiness);
    } catch (error) {
        res.status(503).json({
            status: 'DOWN',
            error: error.message
        });
    }
});

// 3. Queue & System Metrics Monitoring Endpoint
router.get(['/metrics', '/monitoring'], async (req, res) => {
    try {
        const metrics = await healthService.getSystemMetrics();
        res.status(200).json(metrics);
    } catch (error) {
        res.status(500).json({
            error: error.message
        });
    }
});

module.exports = router;
