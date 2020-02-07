const express = require('express')
require('express-async-errors');
const Kafka = require('no-kafka')
const config = require('config')
const bodyParser = require('body-parser')
const {
  create_producer_app_log,
  producerpost_success_log,
  producerpost_failure_log
} = require('./common/app_log')
const pushToKafka = require('./api/pushToKafka')
const { 
  postMessage,
  validateMsgPosted
} = require('./api/postslackinfo')

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
  //retry_count  = payload['RETRY_COUNT'] ? payload['RETRY_COUNT'] : 0
  //let reconcile_flag = payload['RECONCILE_STATUS'] ? payload['RECONCILE_STATUS'] : 0
  let producer_retry_count

  try {
    await create_producer_app_log(payload,"PayloadReceived")
  } catch (error) {
    console.log(error)
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
    await producerpost_success_log(payload, "PayloadPosted")
    res.send('done')
    return
  }

  //add auditlog
  await producerpost_failure_log(payload,kafka_error,'PayloadPostFailed')
  
  msgValue = {
    ...kafka_error,
    SEQ_ID: seqID,
    recipients: config.topic_error.EMAIL,
    msgoriginator: "producer"
  }
  //send error message to kafka
  kafka_error = await pushToKafka(producer, config.topic_error.NAME, msgValue)
  if (!kafka_error) {
    console.log("Kafka Message posted successfully to the topic : " + config.topic_error.NAME)
  } else {
    if (config.SLACK.SLACKNOTIFY === 'true') {
      postMessage("producer post meesage failed- But usable to post the error in kafka error topic due to errors", (response) => {
          await validateMsgPosted(response.statusCode,response.statusMessage)
      });
    }
  }

  res.send('error')

})


const producer = new Kafka.Producer()

producer.init().then(function () {
    console.log('connected to local kafka server on port 9092 ...');

    // start the server
    app.listen(config.PORT);
    console.log('Server started! At http://localhost:' + config.PORT);

  } //end producer init
).catch(e => {
  console.log('Error : ', e)
});