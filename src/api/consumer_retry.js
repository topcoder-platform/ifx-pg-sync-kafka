const config = require('config');
const logger = require('../common/logger')
const app_log = require('../common/app_log')
const pushToKafka = require('./pushToKafka')
const slack = require('./postslackinfo')
async function consumerretry(producer, payload) {
  payload['retryCount'] = payload.retryCount + 1;
  //add producer_log
  try {
    await app_log.create_producer_app_log(payload, "ConsumerRetry")
  } catch (error) {
    logger.logFullError(error)
  }
  kafka_error = await pushToKafka(producer, config.topic.NAME, payload)
  //add auditlog
  if (!kafka_error) {
    await app_log.producerpost_success_log(payload, "ConsumerReposted")
  } else {
    //add auditlog
    await app_log.producerpost_failure_log(payload, kafka_error, 'ConsumerRepostFailed')
    msgValue = {
      ...kafka_error,
      SEQ_ID: payload.SEQ_ID,
      recipients: config.topic_error.EMAIL,
      msgoriginator: "consumer-producer"
    }
    //send error message to kafka
    kafka_error = await pushToKafka(producer, config.topic_error.NAME, msgValue)
    if (!kafka_error) {
      logger.info("Kafka Message posted successfully to the topic : " + config.topic_error.NAME)
    } else {
      if (config.SLACK.SLACKNOTIFY === 'true') {
        await slack.postMessage("consumer repost failed - But unable to post message in kafka error topic due to errors", async (response) => {
          await slack.validateMsgPosted(response.statusCode, response.statusMessage)
        });
      }
    }

  }
}

module.exports = consumerretry