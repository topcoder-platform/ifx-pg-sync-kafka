const express = require('express')
const bodyParser = require('body-parser')
const dynamodblib = require('./api/migratedynamodb')
const config = require('config');
const logger = require('./common/logger')
const slack = require('./api/postslackinfo')
const auditlogdb = require('./api/auditlogdboperation')
const pgwrapper = require('./common/postgresWrapper');
database = config.get('POSTGRES.database');
const pgclient = pgwrapper.createPool(database);
pgclient.on('remove', client => {
  logger.debug("setting property to on query completion");
})
logger.debug(pgclient);
const app = express()
const port = process.env.PORT || 8080;
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
  extended: true
}));
app.get('/', function (req, res) {
  res.send('hello world')
})

app.post('/fileevents', async function (req, res) {
  const payload = req.body
  logger.debug({
    topic: config.topic.NAME,
    partition: config.topic.PARTITION,
    message: {
      value: JSON.stringify(payload)
    }
  });
  let received_seqID = payload.TIME + "_" + payload.TABLENAME
  logger.info(`Recieved Payload SeqID : ${received_seqID}`);
  await dynamodblib.pushToDynamoDb(payload)
    .then(async function () {
      await auditlogdb.insertddauditlogdb(pgclient, payload)
        .then(function (ddaudit_insert_status) {
          logger.debug(ddaudit_insert_status)
          res.send('done');
        })
        .catch(function (pgresponse) {
          logger.logFullError(pgresponse);
          notify_msg = `Origniator : IFX-PG Secondary producer \n` +
            `SequnceId : ${received_seqID} \n` +
            `Status : Failed at PG Auditlog update `
          slack.send_msg_to_slack(notify_msg);
          res.send('ddbdone')
        })
    })
    .catch(function (dynamoresponse) {
      logger.logFullError(dynamoresponse);
      notify_msg = `Origniator : IFX-PG Secondary producer \n` +
        `SequnceId : ${received_seqID} \n` +
        `Status : Failed at Dynamo update `
      slack.send_msg_to_slack(notify_msg);
      res.send('failed')
    })
})
app.listen(port);
logger.info('Server started! At http://localhost:' + port);