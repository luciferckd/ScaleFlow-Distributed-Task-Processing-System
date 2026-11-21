/**
 * ScaleFlow API Gateway - Structured Logger
 * Emits standard JSON logs in production, readable text in local development.
 */

const LOG_FORMAT = (process.env.LOG_FORMAT || (process.env.NODE_ENV === 'production' ? 'json' : 'text')).toLowerCase();
const LOG_LEVEL = (process.env.LOG_LEVEL || 'info').toLowerCase();

const LEVELS = {
    debug: 10,
    info: 20,
    warn: 30,
    error: 40
};

function shouldLog(level) {
    const current = LEVELS[LOG_LEVEL] || LEVELS.info;
    const target = LEVELS[level] || LEVELS.info;
    return target >= current;
}

function formatLog(level, message, meta = {}) {
    const timestamp = new Date().toISOString();

    if (LOG_FORMAT === 'json') {
        const logObj = {
            timestamp,
            level: level.toUpperCase(),
            service: 'scaleflow-api',
            message,
            ...meta
        };
        return JSON.stringify(logObj);
    }

    const eventTag = meta.event ? `[${meta.event}] ` : '';
    const taskTag = meta.taskId ? `[Task ${meta.taskId}] ` : '';
    return `${timestamp} ${level.toUpperCase().padEnd(5)} ${eventTag}${taskTag}${message}`;
}

const logger = {
    debug(message, meta) {
        if (shouldLog('debug')) console.debug(formatLog('debug', message, meta));
    },
    info(message, meta) {
        if (shouldLog('info')) console.log(formatLog('info', message, meta));
    },
    warn(message, meta) {
        if (shouldLog('warn')) console.warn(formatLog('warn', message, meta));
    },
    error(message, meta) {
        if (shouldLog('error')) console.error(formatLog('error', message, meta));
    }
};

module.exports = logger;
