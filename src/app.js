/**
 * The application entry point
 */

global.Promise = require('bluebird')
const config = require('config')
const logger = require('./common/logger')
const {
    getInformixConnection,
    prepare,
    wrapTransaction
} = require('./common/helper')

logger.debug('DISABLE_LOGGING :: ' + config.DISABLE_LOGGING)
logger.info(`Verifying the database`)


async function play() {

    let sql = config.get('INFORMIX.sqlquery')

    const connectionString = 'SERVER=' + config.get('INFORMIX.SERVER') +
        ';DATABASE=' + config.get('INFORMIX.DATABASE') +
        ';HOST=' + config.get('INFORMIX.HOST') +
        ';Protocol=' + config.get('INFORMIX.PROTOCOL') +
        ';SERVICE=' + config.get('INFORMIX.PORT') +
        ';DB_LOCALE=' + config.get('INFORMIX.DB_LOCALE') +
        ';UID=' + config.get('INFORMIX.USER') +
        ';PWD=' + config.get('INFORMIX.PASSWORD')

    try {
        const connection = await getInformixConnection()
        const queryStmt = await prepare(connection, sql)
        const queryResult = Promise.promisifyAll((await queryStmt.executeAsync()))
        const result = await queryResult.fetchAllAsync();
        logger.info(result);
        connection.closeAsync();
    } catch (e) {
        logger.error('Error')
        logger.logFullError(e)
    }
}
play()