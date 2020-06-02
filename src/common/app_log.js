const auditlog = require('../api/audit')
const logger = require('./logger')

async function create_producer_app_log(payload,overallstatus) {
    let seqID = payload.TIME + "_" + payload.TABLENAME
    let reconcile_flag = payload['RECONCILE_STATUS'] ? payload['RECONCILE_STATUS'] : 0
    let producer_retry_count
    if (reconcile_flag == 0) {
        if (!payload.retryCount) {
            producer_retry_count = 0
        } else {
            producer_retry_count = payload.retryCount
        }
    } else {
        producer_retry_count = 100 + reconcile_flag
    }

    try {
        await auditlog.producerLog({
                SEQ_ID: seqID,
                PRODUCER_PAYLOAD: payload,
                PODUCER_PUBLISH_RETRY_COUNT: producer_retry_count
            }).then(log => logger.info('Payload updated to Producer table'))
            .catch(err => logger.logFullError(err))
        if ((reconcile_flag == 0) && (producer_retry_count == 0)) {
            await auditlog.pAuditLog({
                SEQ_ID: seqID,
                REQUEST_CREATE_TIME: Date.now(),
                PRODUCER_PAYLOAD: payload,
                PODUCER_PUBLISH_RETRY_COUNT: producer_retry_count,
                OVERALL_STATUS: overallstatus,
                RECONCILE_STATUS: reconcile_flag
            }).then((log) => logger.info('updated the  auditlog'))
        } else {
            await auditlog.pAuditLog_update({
                SEQ_ID: seqID,
                PRODUCER_PAYLOAD: payload,
                PODUCER_PUBLISH_RETRY_COUNT: producer_retry_count,
                OVERALL_STATUS: overallstatus,
                RECONCILE_STATUS: reconcile_flag
            }).then((log) => logger.info('updated the  auditlog'))
        }


    } catch (error) {
        logger.logFullError(error)
    }

    logger.info('ProducerLog Success')

}

async function producerpost_success_log(payload , overallstatus) {
    let seqID = payload.TIME + "_" + payload.TABLENAME
    await auditlog.pAuditLog_update({
        SEQ_ID: seqID,
        PRODUCER_PUBLISH_STATUS: 'success',
        PRODUCER_PUBLISH_TIME: Date.now(),
        OVERALL_STATUS: overallstatus
    }).then((log) => logger.info('Send Success'))

    const logMessage = `${seqID} ${payload.TABLENAME} ${payload.uniquedatatype} ${payload.OPERATION} ${payload.TIME}`
    logger.debug(`producer : ${logMessage}`);
}

async function producerpost_failure_log(payload, kafka_error, overallstatus) {
    let producer_retry_count
    let seqID = payload.TIME + "_" + payload.TABLENAME
    let reconcile_flag = payload['RECONCILE_STATUS'] ? payload['RECONCILE_STATUS'] : 0
    if (reconcile_flag == 0) {
        if (!payload.retryCount) {
            producer_retry_count = 0
        } else {
            producer_retry_count = payload.retryCount
        }
    } else {
        producer_retry_count = 100 + reconcile_flag
    }
    await auditlog.pAuditLog_update({
        SEQ_ID: seqID,
        PRODUCER_PUBLISH_STATUS: 'failure',
        PRODUCER_FAILURE_LOG: kafka_error,
        PRODUCER_PUBLISH_TIME: Date.now(),
        OVERALL_STATUS: overallstatus
    }).then((log) => logger.info('Send Failure'))

    await auditlog.producerLog_update({
            SEQ_ID: seqID,
            PRODUCER_FAILURE_LOG: kafka_error,
            PODUCER_PUBLISH_RETRY_COUNT: producer_retry_count
        }).then(log => logger.info('Payload updated to Producer table'))
        .catch(err => logger.logFullError(err))

    logger.debug(`error-sync: producer parse message : "${kafka_error}"`)    
}

async function create_consumer_app_log(payload) {
    let consumer_retry_count
    let reconcile_flag = payload['RECONCILE_STATUS'] ? payload['RECONCILE_STATUS'] : 0
    if (reconcile_flag == 0) {
        if (!payload.retryCount) {
            consumer_retry_count = 0
        } else {
            consumer_retry_count = payload.retryCount
        }
    } else {
        consumer_retry_count = 100 + reconcile_flag
    }


    try {
        logger.info("payload sequece ID : " + payload.SEQ_ID)
        await auditlog.consumerLog({
                SEQ_ID: payload.SEQ_ID,
                CONSUMER_UPDATE_RETRY_COUNT: consumer_retry_count,
                CONSUMER_PAYLOAD: payload
            }).then(log => logger.info('Added Consumer Log'))
            .catch(err => logger.logFullError(err))

        await auditlog.cAuditLog({
                SEQ_ID: payload.SEQ_ID,
                CONSUMER_PAYLOAD: payload,
                CONSUMER_UPDATE_RETRY_COUNT: consumer_retry_count,
                OVERALL_STATUS: 'ConsumerReceved'
            }).then(log => logger.info('Added consumer audit log successfully'))
            .catch(err => logger.logFullError(err))
    } catch (error) {
        logger.logFullError(error)
    }
}

async function consumerpg_success_log(payload) {
    await auditlog.cAuditLog({
            SEQ_ID: payload.SEQ_ID,
            CONSUMER_DEPLOY_STATUS: 'success',
            CONSUMER_UPDATE_TIME: Date.now(),
            OVERALL_STATUS: 'PostgresUpdated'
        }).then(log => logger.info('postgres ' + payload.OPERATION + ' success'))
        .catch(err => logger.logFullError(err))
        const logMessage = `${payload.SEQ_ID} ${payload.TABLENAME} ${payload.uniquedatatype} ${payload.OPERATION} ${payload.TIME}`
        logger.debug(`consumer : ${logMessage}`);        
}

async function consumerpg_failure_log(payload, postgreErr) {
    //consumser table
    let consumer_retry_count
    //let reconcile_flag = payload['RECONCILE_STATUS'] ? payload['RECONCILE_STATUS'] : 0
    let reconcile_flag = payload['RECONCILE_STATUS'] ? payload['RECONCILE_STATUS'] : 0
    if (reconcile_flag == 0) {
        if (!payload.retryCount) {
            consumer_retry_count = 0
        } else {
            consumer_retry_count = payload.retryCount
        }
    } else {
        consumer_retry_count = 100
    }
    await auditlog.cAuditLog({
            SEQ_ID: payload.SEQ_ID,
            CONSUMER_DEPLOY_STATUS: 'failure',
            CONSUMER_FAILURE_LOG: postgreErr,
            CONSUMER_UPDATE_TIME: Date.now(),
            CONSUMER_UPDATE_RETRY_COUNT: consumer_retry_count,
            OVERALL_STATUS: 'PostgresUpdateFailed'

        }).then((log) => logger.info('postgres ' + payload.OPERATION + ' failure'))
        .catch(err => logger.logFullError(err))
    await auditlog.consumerLog_update({
            SEQ_ID: payload.SEQ_ID,
            CONSUMER_UPDATE_RETRY_COUNT: consumer_retry_count,
            CONSUMER_FAILURE_LOG: postgreErr
        }).then(log => logger.info('Added Error in Consumer Log Table'))
        .catch(err => logger.logFullError(err))
    logger.debug(`error-sync: consumer failed to update :` + JSON.stringify(postgreErr))    
    //audit table update
}

module.exports = {
    create_producer_app_log,
    producerpost_success_log,
    producerpost_failure_log,
    create_consumer_app_log,
    consumerpg_success_log,
    consumerpg_failure_log
}