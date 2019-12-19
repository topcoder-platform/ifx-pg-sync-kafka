const path = require('path');
module.exports = {
  LOG_LEVEL: process.env.LOG_LEVEL || 'debug',
  LOG_FILE: path.join(__dirname, '../app.log'),
  PORT: process.env.PORT || 8080,
  topic: {
    NAME: 'db.ifxpgmigrate.sync',
    PARTITION: 0
  },
  RETRY_COUNTER: 3,
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
  }    
}
