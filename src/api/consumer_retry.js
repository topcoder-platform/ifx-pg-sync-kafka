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
  await sleep(config.KAFKA_REPOST_DELAY)
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
      msgoriginator: "consumer-producer",
      msginfo: "Failed to post message"
    }
    //send error message to kafka
    kafka_error = await pushToKafka(producer, config.topic_error.NAME, msgValue)
    if (!kafka_error) {
      logger.info("Kafka Message posted successfully to the topic : " + config.topic_error.NAME)
    } else {
      notify_msg = `Originator : IFX-PG Consumer \n` +
        `SequnceId : ${payload.SEQ_ID} \n` +
        `Status : Consumer-Retry Kafka pulish message failed. Also unable to post the error in kafka error topic due to errors`
      await slack.send_msg_to_slack(notify_msg);
    }

  }
}
async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
module.exports = consumerretry