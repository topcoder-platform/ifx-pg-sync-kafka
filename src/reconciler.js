const config = require('config')
const cron = require("node-cron");
const express = require("express");
const pg = require('pg')
const request = require("request");
const pgOptions = config.get('POSTGRES')
const database = 'auditlog'
const pgConnectionString = `postgresql://${pgOptions.user}:${pgOptions.password}@${pgOptions.host}:${pgOptions.port}/${database}`
const logger = require('./common/logger')
const _ = require('lodash')
var AWS = require("aws-sdk");
var pgClient
var docClient = new AWS.DynamoDB.DocumentClient({
    region: config.DYNAMODB.REGION,
    convertEmptyValues: true
});
ElapsedTime = config.RECONCILER.RECONCILER_ELAPSE_TIME
var params = {
    TableName: config.DYNAMODB.TABLENAME,
    FilterExpression: "NodeSequenceID between :time_1 and :time_2",
    ExpressionAttributeValues: {
        ":time_1": Date.now() - ElapsedTime,
        ":time_2": Date.now()
    }
}
async function dynamo_pg_validation() {
    return new Promise(async function (resolve, reject) {
        logger.info("scanning");
        await docClient.scan(params, onScan);
        resolve(true)
    });
}
async function onScan(err, data) {
    return new Promise(async function (resolve, reject) {
        if (err) {
            logger.error("Unable to scan the table.")
            logger.logFullError(err);
            reject(err)
        } else {
            logger.info("Scan succeeded.");
            await Promise.all(data.Items.map(async (item) => {
                //data.Items.forEach(async function (item) {
                await validate_data_in_pg(item.SequenceID, item.pl_document)
            }));

            if (typeof data.LastEvaluatedKey != "undefined") {
                logger.info("Scanning for more...");
                params.ExclusiveStartKey = data.LastEvaluatedKey;
                await docClient.scan(params, onScan);
                resolve(true)
            } else {
                resolve(true)
            }
        }
    });
}

async function validate_data_in_pg(SequenceID, payload) {
    return new Promise(async function (resolve, reject) {
        logger.info(`Validating sequence id : ${SequenceID}`);
        const sqlquerytovalidate = 'SELECT COUNT(*) FROM audit_log WHERE "SEQ_ID"=$1';
        const sqlquerytovalidate_values = [SequenceID]
        logger.debug(sqlquerytovalidate);
        await pgClient.query(sqlquerytovalidate, sqlquerytovalidate_values, async (err, res) => {
            if (err) {
                var errmsg0 = `error-sync: Audit reconsiler query  "${err.message}"`
                logger.debug(errmsg0)
                reject(errmsg0)
            } else {
                logger.info("validating whether data exist---------------------");
                const data = res.rows;
                await Promise.all(data.map(async (row) => {
                    if (row['count'] == 0) {
                        logger.info(`posting the topic from dynamodb : ${SequenceID} `);
                        await posttopic(payload, 0)
                        //logger.debug("post the topic");

                    } else {
                        logger.info(`${SequenceID} is exist in pg. So skipping dynamo db post`)
                    }
                    resolve(true)
                }));

            }
        });
    });
}

async function repostfailure() {
    return new Promise(async function (resolve, reject) {
        logger.info("Vaildating audit log on PG");
        rec_ignore_status = config.RECONCILER.RECONCILER_IGNORE_STATUS
        rec_start_elapse = config.RECONCILER.RECONCILER_START_ELAPSE_TIME
        rec_diff_period = config.RECONCILER.RECONCILER_DIFF_PERIOD //Need to be equal to or greater than scheduler time
        rec_interval_type = config.RECONCILER.RECONCILER_DURATION_TYPE
        rec_retry_count = config.RECONCILER.RECONCILER_RETRY_COUNT

        sql1 = `select "SEQ_ID", "PRODUCER_PAYLOAD" from audit_log where "OVERALL_STATUS" not in ($1)`
        sql2 = ` and "REQUEST_CREATE_TIME" between (timezone('utc',now()) - interval '1${rec_interval_type}' * $2)`
        sql3 = ` and  (timezone('utc',now()) - interval '1${rec_interval_type}' * $3)`
        sql4 = ` and "RECONCILE_STATUS" < $4 ;`
        sqltofetchfailure = sql1 + sql2 + sql3 + sql4
        var sqltofetchfailure_values = [rec_ignore_status, rec_diff_period, rec_start_elapse, rec_retry_count]
        logger.info('sql : ', sqltofetchfailure)
        await pgClient.query(sqltofetchfailure, sqltofetchfailure_values, async (err, res) => {
            if (err) {
                var errmsg0 = `error-sync: Audit reconsiler query  "${err.message}"`
                logger.debug(errmsg0)
                reject(err)
                // await callposttoslack(errmsg0)
            } else {
                logger.info("Reposting Data---------------------\n");
                const data = res.rows;
                await Promise.all(data.map(async (row) => {
                    logger.info("\npost the topic for : " + row['SEQ_ID']);
                    await posttopic(row['PRODUCER_PAYLOAD'], 1)
                    resolve(true)
                }));
            }
        });
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
                await postpayload_to_restapi(payload)
                resolve(true)
            } else {
                //post to rest api
                logger.info("Skipping the Reconciler flag");
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

            pgClient = new pg.Client(pgConnectionString)
            try {
                await pgClient.connect()
                logger.debug('Connected to Pg Client2 Audit:')
            } catch (err) {
                logger.error('Could not setup postgres client2')
                logger.logFullError(err)
                process.exit()
            }
            if (config.RECONCILER.RECONCILE_DYNAMODB == 'true') {
                await dynamo_pg_validation()
            }
            //await pgClient.on('end')
            if (config.RECONCILER.RECONCILE_PGSTATUS == 'true') {
                await repostfailure()
            }
            await pgClient.end();
            resolve(true);
        } catch (e) {
            reject(e);
        }
    });
}

const app = express();
const port = process.env.PORT || 8080;
SchedulerTime = config.RECONCILER.RECONCILE_TIMESCHEDULE

cron.schedule(SchedulerTime, function () {
    logger.info("Running task as per schedule");
    main()
});
app.get('/', function (req, res) {
    res.send('hello world')
})
app.listen(port);
logger.info('Server started! At http://localhost:' + port);