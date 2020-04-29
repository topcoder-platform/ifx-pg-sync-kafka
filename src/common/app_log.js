const {
    producerLog,
    producerLog_update,
    pAuditLog,
    pAuditLog_update,
    consumerLog,
    consumerLog_update,
    cAuditLog
} = require('../api/audit')

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
        await producerLog({
                SEQ_ID: seqID,
                PRODUCER_PAYLOAD: payload,
                PODUCER_PUBLISH_RETRY_COUNT: producer_retry_count
            }).then(log => console.log('Payload updated to Producer table'))
            .catch(err => console.log(err))
        if ((reconcile_flag == 0) && (producer_retry_count == 0)) {
            await pAuditLog({
                SEQ_ID: seqID,
                REQUEST_CREATE_TIME: Date.now(),
                PRODUCER_PAYLOAD: payload,
                PODUCER_PUBLISH_RETRY_COUNT: producer_retry_count,
                OVERALL_STATUS: overallstatus,
                RECONCILE_STATUS: reconcile_flag
            }).then((log) => console.log('updated the  auditlog'))
        } else {
            await pAuditLog_update({
                SEQ_ID: seqID,
                PRODUCER_PAYLOAD: payload,
                PODUCER_PUBLISH_RETRY_COUNT: producer_retry_count,
                OVERALL_STATUS: overallstatus,
                RECONCILE_STATUS: reconcile_flag
            }).then((log) => console.log('updated the  auditlog'))
        }


    } catch (error) {
        console.log(error)
    }

    console.log('ProducerLog Success')

}

async function producerpost_success_log(payload , overallstatus) {
    let seqID = payload.TIME + "_" + payload.TABLENAME
    await pAuditLog_update({
        SEQ_ID: seqID,
        PRODUCER_PUBLISH_STATUS: 'success',
        PRODUCER_PUBLISH_TIME: Date.now(),
        OVERALL_STATUS: overallstatus
    }).then((log) => console.log('Send Success'))

    const logMessage = `${seqID} ${payload.TABLENAME} ${payload.uniquedatatype} ${payload.OPERATION} ${payload.TIME}`
    console.log(`producer : ${logMessage}`);
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
    await pAuditLog_update({
        SEQ_ID: seqID,
        PRODUCER_PUBLISH_STATUS: 'failure',
        PRODUCER_FAILURE_LOG: kafka_error,
        PRODUCER_PUBLISH_TIME: Date.now(),
        OVERALL_STATUS: overallstatus
    }).then((log) => console.log('Send Failure'))

    await producerLog_update({
            SEQ_ID: seqID,
            PRODUCER_FAILURE_LOG: kafka_error,
            PODUCER_PUBLISH_RETRY_COUNT: producer_retry_count
        }).then(log => console.log('Payload updated to Producer table'))
        .catch(err => console.log(err))

    console.log(`error-sync: producer parse message : "${kafka_error}"`)    
}

// consumerLog.schema = Joi.object().keys({
//     SEQ_ID: Joi.string().required(),
//     CONSUMER_UPDATE_RETRY_COUNT: Joi.number(),
//     CONSUMER_PAYLOAD: Joi.object(),
//     CONSUMER_FAILURE_LOG: Joi.object()
//   })
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
        console.log("payload sequece ID : " + payload.SEQ_ID)
        await consumerLog({
                SEQ_ID: payload.SEQ_ID,
                CONSUMER_UPDATE_RETRY_COUNT: consumer_retry_count,
                CONSUMER_PAYLOAD: payload
            }).then(log => console.log('Added Consumer Log'))
            .catch(err => console.log(err))

        await cAuditLog({
                SEQ_ID: payload.SEQ_ID,
                CONSUMER_PAYLOAD: payload,
                CONSUMER_UPDATE_RETRY_COUNT: consumer_retry_count,
                OVERALL_STATUS: 'ConsumerReceved'
            }).then(log => console.log('Added consumer audit log successfully'))
            .catch(err => console.log(err))
    } catch (error) {
        console.log(error)
    }
}

async function consumerpg_success_log(payload) {
    let consumer_retry_count
    //let reconcile_flag = payload['RECONCILE_STATUS'] ? payload['RECONCILE_STATUS'] : 0
    // let reconcile_flag = payload['RECONCILE_STATUS'] ? payload['RECONCILE_STATUS'] : 0
    // if (reconcile_flag == 0) {
    //     if (!payload.retryCount) {
    //         consumer_retry_count = 0
    //     } else {
    //         consumer_retry_count = payload.retryCount
    //     }
    // } else {
    //     consumer_retry_count = 100
    // }
    // CONSUMER_UPDATE_RETRY_COUNT: consumer_retry_count,
    await cAuditLog({
            SEQ_ID: payload.SEQ_ID,
            CONSUMER_DEPLOY_STATUS: 'success',
            CONSUMER_UPDATE_TIME: Date.now(),
            OVERALL_STATUS: 'PostgresUpdated'
        }).then(log => console.log('postgres ' + payload.OPERATION + ' success'))
        .catch(err => console.log(err))
        const logMessage = `${payload.SEQ_ID} ${payload.TABLENAME} ${payload.uniquedatatype} ${payload.OPERATION} ${payload.TIME}`
        console.log(`consumer : ${logMessage}`);        
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
    await cAuditLog({
            SEQ_ID: payload.SEQ_ID,
            CONSUMER_DEPLOY_STATUS: 'failure',
            CONSUMER_FAILURE_LOG: postgreErr,
            CONSUMER_UPDATE_TIME: Date.now(),
            CONSUMER_UPDATE_RETRY_COUNT: consumer_retry_count,
            OVERALL_STATUS: 'PostgresUpdateFailed'

        }).then((log) => console.log('postgres ' + payload.OPERATION + ' failure'))
        .catch(err => console.log(err))
    await consumerLog_update({
            SEQ_ID: payload.SEQ_ID,
            CONSUMER_UPDATE_RETRY_COUNT: consumer_retry_count,
            CONSUMER_FAILURE_LOG: postgreErr
        }).then(log => console.log('Added Error in Consumer Log Table'))
        .catch(err => console.log(err))
    console.log(`error-sync: consumer failed to update :` + JSON.stringify(postgreErr))    
    //audit table update
}
// CONSUMER_PAYLOAD: { type: DataTypes.JSON, allowNull: false },    
// CONSUMER_DEPLOY_STATUS: { type: DataTypes.STRING },
// CONSUMER_FAILURE_LOG: { type: DataTypes.JSON },
// CONSUMER_UPDATE_TIME:{ type: DataTypes.DATE },
// CONSUMER_UPDATE_RETRY_COUNT:{ type: DataTypes.INTEGER ,defaultValue: 0 },
// OVERALL_STATUS:{ type: DataTypes.STRING },
module.exports = {
    create_producer_app_log,
    producerpost_success_log,
    producerpost_failure_log,
    create_consumer_app_log,
    consumerpg_success_log,
    consumerpg_failure_log
}