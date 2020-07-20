const config = require('config');
const logger = require('../common/logger')
const dynamo_audittablename = config.AUDITLOG.AUDITLOG_DYNAMO_TABLENAME
const pg_audittablename = config.AUDITLOG.AUDITLOG_PG_TABLENAME
const auditdbname = config.AUDITLOG.AUDITLOG_DBNAME

async function insertddauditlogdb(pgclient, payload) {
  return new Promise(async function (resolve, reject) {
    try {
      let seqID = payload.TIME + "_" + payload.TABLENAME
      logger.debug("db name : " + auditdbname);
      logger.debug("dynamo audit table name : " + dynamo_audittablename);
      sql = `SET search_path TO public;`;
      logger.info(sql);
      await pgclient.query(sql)
      // .then(result => {logger.info(result)});
      sql = `insert into "${dynamo_audittablename}" ("SEQ_ID","PUBLISH_TIME") values($1,$2);`;
      var dynamodbvalues = [];
      dynamodbvalues.push(seqID);
      dynamodbvalues.push(new Date());
      // dynamodbvalues.push(Date.now());
      logger.info("Executing query : " + sql);
      await pgclient.query(sql, dynamodbvalues)
        .then(result => {
          resolve(result)
          logger.info("query executed Successfully")
        })
        .catch(e => {
          reject(e)
          logger.info("query completed with error")
        })
      //logger.info(`end connection of postgres for database after insert`);
    } catch (e) {
      logger.info("Throwing err in insertddauditlogdb");
      reject(e);
      //throw e;
    }
  })
}

async function fetch_dynamo_pg_diff_seqid(pgclient) {
  return new Promise(async function (resolve, reject) {
    try {
      logger.debug("db name : " + auditdbname);
      logger.debug("dynamo audit table name : " + dynamo_audittablename);
      logger.debug("pg audit table name : " + pg_audittablename);
      sql = `SET search_path TO public;`;
      logger.info(sql);
      await pgclient.query(sql);
      var queryvalues = [];
      queryvalues.push(config.RECONCILER.RECONCILER_DYNAMO_DIFF_PERIOD);
      queryvalues.push(config.RECONCILER.RECONCILER_DYNAMO_START_ELAPSE_TIME);
      rec_interval_type = config.RECONCILER.RECONCILER_DYNAMO_DURATION_TYPE
      sql = `select * from "${dynamo_audittablename}" tbl1 ` +
        `where not exists (select from "${pg_audittablename}" where "SEQ_ID" = tbl1."SEQ_ID") ` +
        `and tbl1."PUBLISH_TIME" between (timezone('utc',now()) - interval '1${rec_interval_type}' * $1) and (timezone('utc',now()) - interval '1${rec_interval_type}' * $2);`;
      logger.info("Executing query : " + sql);
      await pgclient.query(sql, queryvalues)
        .then(result => {
          resolve(result)
          logger.info("Diff data fetched successfully")
        })
        .catch(e => {
          reject(e)
          logger.info("Diff data fetched failed")
        })
      //logger.info(`end connection of postgres for database after insert`);

    } catch (e) {
      logger.info("Throwing err in fetch_dynamo_pg_diff_seqid");
      reject(e);
      //throw e;
    }
  })
}

async function fetch_status_failure_seqid(pgclient) {

  return new Promise(async function (resolve, reject) {
    logger.info("Vaildating audit log on PG");
    var sqltofetchfailure_values = []
    //Note the  below array insert sequence should not change
    sqltofetchfailure_values.push(config.RECONCILER.RECONCILER_PG_IGNORE_STATUS);
    sqltofetchfailure_values.push(config.RECONCILER.RECONCILER_PG_DIFF_PERIOD);
    sqltofetchfailure_values.push(config.RECONCILER.RECONCILER_PG_START_ELAPSE_TIME);
    sqltofetchfailure_values.push(config.RECONCILER.RECONCILER_RETRY_COUNT);
    rec_interval_type = config.RECONCILER.RECONCILER_PG_DURATION_TYPE

    sqltofetchfailure = `select "SEQ_ID", "PRODUCER_PAYLOAD" from audit_log ` +
      `where "OVERALL_STATUS" not in ($1) ` +
      `and "REQUEST_CREATE_TIME" between (timezone('utc',now()) - interval '1${rec_interval_type}' * $2) ` +
      `and  (timezone('utc',now()) - interval '1${rec_interval_type}' * $3) ` +
      `and "RECONCILE_STATUS" < $4 ;`
    logger.info('sql : ', sqltofetchfailure)
    await pgclient.query(sqltofetchfailure, sqltofetchfailure_values, async (err, res) => {
      if (err) {
        var errmsg0 = `error-sync: Audit reconsiler query  "${err.message}"`
        logger.debug(errmsg0)
        reject(err)
      } else {
        logger.info("Sync Failure seqid fetched successfully from Auditlig table");
        resolve(res)
      }
    });
  });
}
module.exports = {
  insertddauditlogdb,
  fetch_dynamo_pg_diff_seqid,
  fetch_status_failure_seqid
}