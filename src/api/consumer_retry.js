const config = require('config');
const {
    create_producer_app_log,
    producerpost_success_log,
    producerpost_failure_log
  } = require('../common/app_log')
const pushToKafka = require('./pushToKafka')
const {
    postMessage,
    validateMsgPosted
  } = require('./postslackinfo')
async function consumerretry(producer, payload)
{
    payload['retryCount'] = payload.retryCount + 1;
    //add producer_log
    try {
      await create_producer_app_log(payload, "ConsumerRetry")
    } catch (error) {
      console.log(error)
    }
    kafka_error = await pushToKafka(producer, config.topic.NAME, payload)
    //add auditlog
    if (!kafka_error) {
      await producerpost_success_log(payload, "ConsumerReposted")
      res.send('done')
      //res.send('done')
    } else {
      //add auditlog
      await producerpost_failure_log(payload, kafka_error, 'ConsumerRepostFailed')
      msgValue = {
        ...kafka_error,
        SEQ_ID: payload.SEQ_ID,
        recipients: config.topic_error.EMAIL,
        msgoriginator: "consumer-producer"
      }
      //send error message to kafka
      kafka_error = await pushToKafka(producer, config.topic_error.NAME, msgValue)
      if (!kafka_error) {
        console.log("Kafka Message posted successfully to the topic : " + config.topic_error.NAME)
      } else {
        if (config.SLACK.SLACKNOTIFY === 'true') {
          await postMessage("consumer repost failed - But unable to post message in kafka error topic due to errors", async (response) => {
            await validateMsgPosted(response.statusCode, response.statusMessage)
          });
        }
      }

    }
}

module.exports = consumerretry