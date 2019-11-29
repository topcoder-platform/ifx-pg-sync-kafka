/**
 * This module defines a winston logger instance for the application.
 */
const { createLogger, format, transports } = require('winston')

const config = require('config');

const logger = createLogger({
    format: format.combine(
        format.printf((info) => `${new Date().toISOString()} - ${info.level}: ${info.message}`)
    ),
    transports: [
        new transports.Console({
            stderrLevels: ['error'],
            level: config.LOG_LEVEL
        }),
        new transports.File({ filename: config.LOG_FILE })
    ]
});

logger.logFullError = (err) => {
    if (err && err.stack) {
        logger.error(err.stack)
    } else {
        logger.error(JSON.stringify(err))
    }
};
module.exports = logger;
