const express = require('express')
require('express-async-errors');
const Kafka = require('no-kafka')
const config = require('config')
const bodyParser = require('body-parser')
const { producerLog, pAuditLog } = require('./api/audit')

const app = express()
app.use(bodyParser.json());       // to support JSON-encoded bodies
app.use(bodyParser.urlencoded({     // to support URL-encoded bodies
  extended: true
}));
app.get('/', function (req, res) {
  res.send('hello world')
})


app.post('/events', async (req, res, next) => {
  const payload = req.body
  let seqID = 0

  //add producer_log
  await producerLog({
    TOPICNAME: config.topic.NAME,
    SOURCE: config.SOURCE,
    SCHEMA_NAME: payload.SCHEMANAME,
    TABLE_NAME: payload.TABLENAME,
    PRODUCER_PAYLOAD: payload.DATA,
    OPERATION: payload.OPERATION
  }).then((log) => seqID = log.SEQ_ID)

  if(!seqID){
    console.log('ProducerLog Failure')
    return
  }
  console.log('ProducerLog Success')

  //send kafka message
  let kafka_error
  let msgValue = {
    ...payload,
    SEQ_ID: seqID
  }

  await producer.send({
    topic: config.topic.NAME,
    partition: config.topic.PARTITION,
    message: {
      value : JSON.stringify(msgValue)
    }
  },{
    retries: {
      attempts: config.RETRY_COUNTER,
      delay: {
        min: 100,
        max: 300
      }
    }
  }).then(function (result) {
      if(result[0].error)
        kafka_error = result[0].error
  })

  //add auditlog
  if(!kafka_error){
  await pAuditLog({
    SEQ_ID: seqID,
    PRODUCER_PUBLISH_STATUS: 'success',
    PRODUCER_PUBLISH_TIME: Date.now()
  }).then((log) => console.log('Send Success'))
  res.send('done')
  return
  }

  //add auditlog
  await pAuditLog({
    SEQ_ID: seqID,
    PRODUCER_PUBLISH_STATUS: 'failure',
    PRODUCER_FAILURE_LOG: kafka_error,
    PRODUCER_PUBLISH_TIME: Date.now()
  }).then((log) => console.log('Send Failure'))

  msgValue = {
    ...kafka_error,
    SEQ_ID: seqID,
    recipients: config.topic_error.EMAIL
  }

  //send error message to kafka
  await producer.send({
    topic: config.topic_error.NAME,
    partition: config.topic_error.PARTITION,
    message: {
      value : JSON.stringify(msgValue),
    }
    },{
      retries: {
        attempts: config.RETRY_COUNTER,
        delay: {
          min: 100,
          max: 300
        }
      }
    }).then(function (result) {
      console.log(result)
    })

    res.send('error')

})


const producer = new Kafka.Producer()

producer.init().then(function () {
  console.log('connected to local kafka server on port 9092 ...');

  // start the server
  app.listen(config.PORT);
  console.log('Server started! At http://localhost:' + config.PORT);

} //end producer init
).catch(e => { console.log('Error : ', e) });
