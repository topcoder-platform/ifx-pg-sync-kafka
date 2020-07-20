const path = require('path');
module.exports = {
  DISABLE_LOGGING: false, // If true, logging will be disabled
  LOG_LEVEL: process.env.LOG_LEVEL || 'debug',
  LOG_FILE: path.join(__dirname, '../app.log'),
  PORT: process.env.PORT || 8080,
  topic: {
    NAME: 'db.ifxpgmigrate.sync',
    PARTITION: 0
  },
  RETRY_COUNTER: parseInt(process.env.RETRY_COUNTER || 3, 10),
  KAFKA_REPOST_COUNT: parseInt(process.env.KAFKA_REPOST_COUNT || 5, 10),
  KAFKA_REPOST_DELAY: parseInt(process.env.KAFKA_REPOST_DELAY || 2000, 10),
  KAFKA_URL: process.env.KAFKA_URL,
  KAFKA_GROUP_ID: process.env.KAFKA_GROUP_ID || 'ifx-pg-consumer',
  KAFKA_CLIENT_CERT: process.env.KAFKA_CLIENT_CERT ? process.env.KAFKA_CLIENT_CERT.replace('\\n', '\n') : null,
  KAFKA_CLIENT_CERT_KEY: process.env.KAFKA_CLIENT_CERT_KEY ?
    process.env.KAFKA_CLIENT_CERT_KEY.replace('\\n', '\n') : null,
  topic_error: {
    NAME: 'db.ifxpgmigrate.error',
    PARTITION: 0,
    EMAIL: 'admin@abc.com'
  },
  SOURCE: 'INFORMIX',
  DESTINATION: 'POSTGRESQL',
  db: {
   // URL: "postgres://postgres:password@host.docker.internal:54321/",
    OPTIONS: {
      logging: true,
      operatorsAliases: false,
      dialectOptions: { encrypt: true }
    },
    DB_NAME:['auditlog']
  },
  POSTGRES: {
    user: process.env.PG_USER || 'postgres',
    host: process.env.PG_HOST || 'host.docker.internal',
    database: process.env.PG_DATABASE || 'testdb', // database must exists before run tool
    password: process.env.PG_PASSWORD || 'password',
    port: parseInt(process.env.PG_PORT || 54321, 10),
  },
  INFORMIX: {
    SERVER: process.env.IFX_SERVER || 'informixoltp_tcp',
    DATABASE: process.env.IFX_DATABASE || 'tcs_catalog',
    HOST: process.env.INFORMIX_HOST || 'host.docker.internal',
    PROTOCOL: process.env.IFX_PROTOCOL || 'onsoctcp',
    PORT: process.env.IFX_PORT || '2021',
    DB_LOCALE: process.env.IFX_DB_LOCALE || 'en_US.utf8',
    USER: process.env.IFX_USER || 'informix',
    PASSWORD: process.env.IFX_PASSWORD || '1nf0rm1x',
    POOL_MAX_SIZE: parseInt(process.env.IFX_POOL_MAX_SIZE || '10')
  },
  DYNAMODB: {
    REGION: process.env.AWS_REGION || 'us-east-1',
    TABLENAME: process.env.DYNAMODB_TABLENAME || 'ifxpg-migrator'
  },
  SLACK: {
    URL: process.env.SLACKURL || 'us-east-1',
    SLACKCHANNEL: process.env.SLACKCHANNEL || 'ifxpg-migrator',
    SLACKNOTIFY:  process.env.SLACKNOTIFY || 'false'
  },
  EXEMPTIONDATATYPE : {
    MONEY: {
      testdb_testtable5 : 'dmoney'
    }
  },
  RECONCILER : 
  {
    // Dynamodb configuration
    RECONCILER_ELAPSE_TIME : parseInt(process.env.RECONCILER_ELAPSE_TIME || 600000),
    RECONCILER_DYNAMO_START_ELAPSE_TIME : parseInt(process.env.RECONCILER_DYNAMO_START_ELAPSE_TIME || 1),
    RECONCILER_DYNAMO_DIFF_PERIOD : parseInt(process.env.RECONCILER_DYNAMO_DIFF_PERIOD || 10),
    RECONCILER_DYNAMO_DURATION_TYPE : process.env.RECONCILER_DYNAMO_DURATION_TYPE || 'm',
    //PG Configuration
    RECONCILER_PG_IGNORE_STATUS :  process.env.RECONCILER_PG_IGNORE_STATUS || 'PostgresUpdated',
    RECONCILER_PG_START_ELAPSE_TIME : parseInt(process.env.RECONCILER_PG_START_ELAPSE_TIME || 1),
    RECONCILER_PG_DIFF_PERIOD : parseInt(process.env.RECONCILER_PG_DIFF_PERIOD || 10),
    RECONCILER_PG_DURATION_TYPE : process.env.RECONCILER_PG_DURATION_TYPE || 'm',
    //General Configuration
    RECONCILER_RETRY_COUNT : parseInt(process.env.RECONCILER_RETRY_COUNT || 1),
    RECONCILER_POST_URL : process.env.RECONCILER_POST_URL || 'http://ifxpg-migrator.topcoder-dev.com/kafkaevents',
    RECONCILE_DYNAMODB : process.env.RECONCILE_DYNAMODB || 'true',
    RECONCILE_PGSTATUS : process.env.RECONCILE_PGSTATUS || 'true',
    RECONCILE_TIMESCHEDULE : process.env.RECONCILE_TIMESCHEDULE || '*/3 * * * *'
  },
  AUDITLOG: {
    AUDITLOG_DBNAME: process.env.AUDITLOG_DBNAME || 'auditlog',
    AUDITLOG_DYNAMO_TABLENAME: process.env.AUDITLOG_DYNAMO_TABLENAME || 'dynamodb_audit_log',  
    AUDITLOG_PG_TABLENAME: process.env.AUDITLOG_PG_TABLENAME || 'audit_log'
  } 
}
