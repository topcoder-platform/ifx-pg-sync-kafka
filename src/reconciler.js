const config = require('config')
//Establishing connection in postgress
const pg = require('pg')
const request = require("request");
const pgOptions = config.get('POSTGRES')
const database = 'auditlog'
const pgConnectionString = `postgresql://${pgOptions.user}:${pgOptions.password}@${pgOptions.host}:${pgOptions.port}/${database}`
console.log(pgConnectionString)
//const pgClient = new pg.Client(pgConnectionString)
//pgClient.connect();
const logger = require('./src/common/logger')
const _ = require('lodash')
var AWS = require("aws-sdk");

async function dynamo_pg_validation() {
    var docClient = new AWS.DynamoDB.DocumentClient({
        region: config.DYNAMODB.REGION,
        convertEmptyValues: true
    });

    //ElapsedTime = 600000
    ElapsedTime = 4995999000
    var params = {
        TableName: config.DYNAMODB.TABLENAME,
        FilterExpression: "NodeSequenceID between :time_1 and :time_2",
        ExpressionAttributeValues: {
            ":time_1": Date.now() - ElapsedTime,
            ":time_2": Date.now()
        }
    }
    console.log("scanning");
    await docClient.scan(params, onScan);
    return
}
async function onScan(err, data) {
    if (err) {
        console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
    } else {

        console.log("Scan succeeded.");
        data.Items.forEach(async function (item) {
            //            console.log(item);
            await validate_data_in_pg(item.SequenceID, item.pl_document)

        });

        // continue scanning if we have more movies, because
        // scan can retrieve a maximum of 1MB of data

        if (typeof data.LastEvaluatedKey != "undefined") {
            console.log("Scanning for more...");
            params.ExclusiveStartKey = data.LastEvaluatedKey;
            await docClient.scan(params, onScan);
        } else {
            return
        }
    }
}

async function validate_data_in_pg(SequenceID, payload) {
    const pgClient = new pg.Client(pgConnectionString)
    //  await setupPgClient()
    try {
        await pgClient.connect()
        logger.debug('Connected to Pg Client2 Audit:')
    } catch (err) {
        logger.error('Could not setup postgres client2')
        logger.logFullError(err)
        process.exit()
    }
    console.log(SequenceID);
    let schemaname = 'public';
    const sqlquerytovalidate = 'SELECT COUNT(*) FROM audit_log WHERE seq_id=$1';
    const sqlquerytovalidate_values = [SequenceID]
    console.log(sqlquerytovalidate);
    await pgClient.query(sqlquerytovalidate, sqlquerytovalidate_values, async (err, res) => {

        if (err) {
            var errmsg0 = `error-sync: Audit reconsiler query  "${err.message}"`
            logger.debug(errmsg0)
            // await callposttoslack(errmsg0)
        } else {
            console.log("validating data count---------------------");
            const data = res.rows;
            data.forEach(async (row) => {
                if (row['count'] == 0) {
                    await posttopic(payload, 0)
                    console.log("post the topic");
                } else {
                    console.log(`${SequenceID} is exist in pg`)
                }
            });
        }
        pgClient.end();
    });
    return
}

async function repostfailure() {
    const pgClient = new pg.Client(pgConnectionString)
    //  await setupPgClient()
    try {
        await pgClient.connect()
        logger.debug('Connected to Pg Client2 Audit:')
    } catch (err) {
        logger.error('Could not setup postgres client2')
        logger.logFullError(err)
        process.exit()
    }

    //      select seq_id, producer_payload, overall_status from audit_log where 
    // overall_status not in ('PostgresUpdated') and 
    // request_create_time between (timezone('utc',now()) - interval '10m') and (timezone('utc',now()) - interval '1m') and
    // reconcile_status < 1 ;
    rec_ignore_status = config.RECONCILER.RECONCILER_IGNORE_STATUS
    rec_start_elapse = config.RECONCILER.RECONCILER_START_ELAPSE_TIME
    rec_diff_period = config.RECONCILER.RECONCILER_DIFF_PERIOD
    rec_interval_type = config.RECONCILER.RECONCILER_DURATION_TYPE
    rec_retry_count = config.RECONCILER.RECONCILER_RETRY_COUNT

    sql1 = "select seq_id, producer_payload from audit_log where audit_log.overall_status not in ($1)"
    sql2 = " and audit_log.request_create_time between (timezone('utc',now()) - interval '1" + rec_interval_type + "' * $2)"
    sql3 = " and  (timezone('utc',now()) - interval '1" + rec_interval_type + "' * $3)"
    sql4 = " and audit_log.reconcile_status < $4 ;"
    sqltofetchfailure = sql1 + sql2 + sql3 + sql4
    var sqltofetchfailure_values = [rec_ignore_status, rec_diff_period, rec_start_elapse, rec_retry_count]
    console.log('sql : ', sqltofetchfailure)
    await pgClient.query(sqltofetchfailure, sqltofetchfailure_values, async (err, res) => {

        if (err) {
            var errmsg0 = `error-sync: Audit reconsiler query  "${err.message}"`
            logger.debug(errmsg0)
            // await callposttoslack(errmsg0)
        } else {
            console.log("Reposting Data---------------------\n");
            const data = res.rows;
            data.forEach(async (row) => {
                console.log("\npost the topic for : " + row['seq_id']);
                await posttopic(row['producer_payload'], 1)
            });
        }
        pgClient.end();
    });
    return

}

async function postpayload_to_restapi(payload) {
    let options = {
        method: 'POST',
        url: config.RECONCILER.RECONCILER_POST_URL,
        headers: {
            'cache-control': 'no-cache',
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload),
        json: true
    };

    request(options, function (error, response, body) {
        if (error) throw new Error(error);
        console.log(body);
    });
    return
}

async function posttopic(payload, integratereconcileflag) {
    console.log(payload + " " + integratereconcileflag);
    if (integratereconcileflag == 1) {
        //update payload with reconcile status
        //post to rest api
        let reconcile_flag = payload['RECONCILE_STATUS'] ? payload['RECONCILE_STATUS'] : 0
        reconcile_flag = reconcile_flag + 1
        payload.RECONCILE_STATUS = reconcile_flag
        await postpayload_to_restapi(payload)
    } else {
        //post to rest api
        await postpayload_to_restapi(payload)
    }
    return
}
async function main() {
    //await setupPgClient()
    await dynamo_pg_validation()
    //await pgClient.on('end')
    await repostfailure()

}
main()
