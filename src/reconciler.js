const config = require('config')
const cron = require("node-cron");
const express = require("express");
const request = require("request");
const logger = require('./common/logger')
const _ = require('lodash')
const pgwrapper = require('./common/postgresWrapper');
const auditlogoperation = require('./api/auditlogdboperation')
const dynamodblib = require('./api/migratedynamodb')
var pgclient = null
async function setup_globalobject() {
    try {
        database = config.get('POSTGRES.database');
        pgclient = pgwrapper.createPool(database);
        pgclient.on('remove', client => {
            logger.debug("setting property to on query completion");
        })
    } catch (e) {
        logger.logFullError(e);
        terminate();
    }
}

async function dynamo_pg_validation() {
    return new Promise(async function (resolve, reject) {
        logger.info("scanning");
        await auditlogoperation.fetch_dynamo_pg_diff_seqid(pgclient)
            .then(async function (diffdata) {
                //console.log(diffdata)
                var data = diffdata.rows;
                if (data === undefined || data.length == 0) {
                    logger.info("No missing seqid identitfied while comparing dynamodb_audit_log with audit_log")
                    resolve(true);
                } else {
                    await Promise.all(data.map(async (row) => {
                        logger.info("Posting sequence id : " + row['SEQ_ID']);
                        await dynamodblib.getdata_DynamoDb(row['SEQ_ID'])
                            .then(async function (dydata) {
                                //logger.debug(dydata)
                                await Promise.all(dydata.Items.map(async (item) => {
                                    logger.debug("items retrived dynamodb : " + JSON.stringify(item));
                                    logger.info("Item seqid retrived dynamodb : " + item.SequenceID);
                                    await posttopic(item.pl_document, 0)
                                    resolve(true)
                                }));
                            })
                            .catch(function (err) {
                                logger.logFullError(err);
                                reject(err)
                            })
                        resolve(true)
                    }));
                    resolve(true)
                }
            })
            .catch(function (err) {
                logger.logFullError(err);
                reject(err)
            })
    });
}

async function repostfailure() {
    return new Promise(async function (resolve, reject) {
        logger.info("Vaildating audit log on PG");
        await auditlogoperation.fetch_status_failure_seqid(pgclient)
            .then(async function (failureseqdata) {
                //console.log(failureseqdata)
                const fstatus_data = failureseqdata.rows;
                if (fstatus_data === undefined || fstatus_data.length == 0) {
                    logger.info("No Failure sequeceid identified in auditlog")
                    resolve(true);
                } else {
                    await Promise.all(fstatus_data.map(async (row) => {
                        logger.info("Reposting the sequence id : " + row['SEQ_ID']);
                        await posttopic(row['PRODUCER_PAYLOAD'], 1)
                        resolve(true)
                    }));
                    resolve(true)
                }
            })
            .catch(function (err) {
                logger.logFullError(err);
                reject(err)
            })
    });
}
async function postpayload_to_restapi(payload) {
    return new Promise(async function (resolve, reject) {
        let options = {
            method: 'POST',
            url: config.RECONCILER.RECONCILER_POST_URL,
            headers: {
                'cache-control': 'no-cache',
                'Content-Type': 'application/json'
            },
            body: payload,
            json: true
        };
        request(options, function (error, response, body) {
            if (error) {
                var errmsg0 = `error-sync: Audit Reconsiler1 query  "${error.message}"`
                logger.debug(errmsg0)
                reject(error)
                //throw new Error(error);
            } else {
                logger.info("ReconcilerIFXtoPG :  " + payload['TIME'] + "_" + payload['TABLENAME'] + " Success")
                logger.debug(body);
                resolve(true)
            }
        });
    });
}

async function posttopic(payload, integratereconcileflag) {
    return new Promise(async function (resolve, reject) {
        try {
            logger.debug(payload + " " + integratereconcileflag);
            if (integratereconcileflag == 1) {
                //update payload with reconcile status and post to rest api
                logger.info("Integrated the Reconciler flag");
                let reconcile_flag = payload['RECONCILE_STATUS'] ? payload['RECONCILE_STATUS'] : 0
                reconcile_flag = reconcile_flag + 1
                payload.RECONCILE_STATUS = reconcile_flag
                logger.debug(payload)
                await postpayload_to_restapi(payload)
                resolve(true)
            } else {
                //post to rest api
                logger.info("Skipping the Reconciler flag");
                logger.debug(payload)
                await postpayload_to_restapi(payload)
                resolve(true)
            }
        } catch (errmsg) {
            reject(errmsg)
        }
    });
}
async function main() {
    return new Promise(async function (resolve, reject) {
        try {
            if (config.RECONCILER.RECONCILE_DYNAMODB == 'true') {
                await dynamo_pg_validation()
            }
            if (config.RECONCILER.RECONCILE_PGSTATUS == 'true') {
                await repostfailure()
            }
            resolve(true);
        } catch (e) {
            reject(e);
        }
    });
}

const app = express();
const port = process.env.PORT || 8080;
SchedulerTime = config.RECONCILER.RECONCILE_TIMESCHEDULE
setup_globalobject();
cron.schedule(SchedulerTime, function () {
    logger.info("Running task as per schedule");
    main()
});
app.get('/', function (req, res) {
    res.send('hello world')
})
app.listen(port);
logger.info('Server started! At http://localhost:' + port);

/*
setup_globalobject();
main()
*/