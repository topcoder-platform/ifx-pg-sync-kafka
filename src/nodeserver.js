const express = require('express')
require('express-async-errors');
const Kafka = require('no-kafka')
const config = require('config')
const bodyParser = require('body-parser')
const app_log = require('./common/app_log')
const pushToKafka = require('./api/pushToKafka')
const slack = require('./api/postslackinfo')
const logger = require('./common/logger');
const app = express()
app.use(bodyParser.json()); // to support JSON-encoded bodies
app.use(bodyParser.urlencoded({ // to support URL-encoded bodies
  extended: true
}));
app.get('/', function (req, res) {
  res.send('hello world')
})
app.post('/kafkaevents', async (req, res, next) => {
  const payload = req.body
  let seqID = payload.TIME + "_" + payload.TABLENAME
  try {
    await app_log.create_producer_app_log(payload, "PayloadReceived")
  } catch (error) {
    logger.logFullError(error)
  }
  //send kafka message
  let kafka_error
  let msgValue = {
    ...payload,
    SEQ_ID: seqID
  }
  kafka_error = await pushToKafka(producer, config.topic.NAME, msgValue)
  //add auditlog
  if (!kafka_error) {
    await app_log.producerpost_success_log(payload, "PayloadPosted")
    res.send('done')
    return
  }
  //add auditlog
  await app_log.producerpost_failure_log(payload, kafka_error, 'PayloadPostFailed')
  msgValue = {
    ...kafka_error,
    SEQ_ID: seqID,
    recipients: config.topic_error.EMAIL,
    msgoriginator: "producer"
  }
  //send error message to kafka
  kafka_error = await pushToKafka(producer, config.topic_error.NAME, msgValue)
  if (!kafka_error) {
    logger.info("Kafka Message posted successfully to the topic : " + config.topic_error.NAME)
  } else {
    if (config.SLACK.SLACKNOTIFY === 'true') {
      await slack.postMessage("producer post meesage failed- But usable to post the error in kafka error topic due to errors", async (response) => {
        await slack.validateMsgPosted(response.statusCode, response.statusMessage)
      });
    }
  }
  res.send('error')
})

const producer = new Kafka.Producer()
producer.init().then(function () {
    logger.info('connected to local kafka server on port 9092 ...');
    // start the server
    app.listen(config.PORT);
    logger.info('Server started! At http://localhost:' + config.PORT);
  } //end producer init
).catch(e => {
  logger.error('Error : Kafka producer initialise failed')
  logger.logFullError(e)
});