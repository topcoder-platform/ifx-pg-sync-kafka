const Kafka = require('no-kafka');
const Promise = require('bluebird');
const config = require('config');
const logger = require('./common/logger');
const healthcheck = require('topcoder-healthcheck-dropin');
const app_log = require('./common/app_log')
const migratepg = require('./api/migratepg')
const migrateifxpg = require('./api/migrateifxpg')
const pushToKafka = require('./api/pushToKafka')
const slack = require('./api/postslackinfo')
const consumerretry = require('./api/consumer_retry')
const options = {
  groupId: config.KAFKA_GROUP_ID,
  connectionString: config.KAFKA_URL,
  ssl: {
    cert: config.KAFKA_CLIENT_CERT,
    key: config.KAFKA_CLIENT_CERT_KEY
  }
};

const consumer = new Kafka.GroupConsumer(options);

const producer = new Kafka.Producer()

producer.init().then(function () {
  logger.info('connected to local kafka server on port 9092 ...');
}).catch(e => {
  logger.error(`Error : Kafka producer initial error`)
  logger.logFullError(e)
});

const {
  createPool,
} = require('./common/postgresWrapper');
database = config.get('POSTGRES.database');
const pool = createPool(database);
pool.on('remove', client => {
  logger.debug("setting property to on query completion");
})
logger.debug(`${pool}`);
async function dataHandler(messageSet, topic, partition) {
  return Promise.each(messageSet, async function (m) {
    const payload = JSON.parse(m.message.value)
    try {
      await app_log.create_consumer_app_log(payload)
    } catch (error) {
      logger.logFullError(error)
    }

    //update postgres table
    let postgreErr
    if (payload.uniquedatatype === 'true') {
      //retrive the data from info and insert in postgres
      if (payload.OPERATION === 'INSERT') {
        await migrateifxpg.migrateifxinsertdata(payload, pool)
          .catch(err => {
            postgreErr = err
          })
      }
      if (payload.OPERATION === 'UPDATE') {
        await migrateifxpg.migrateifxupdatedata(payload, pool)
          .catch(err => {
            postgreErr = err
          })
      }
      if (payload.OPERATION === 'DELETE') {
        await migrateifxpg.migrateifxdeletedata(payload, pool)
          .catch(err => {
            postgreErr = err
          })
      }
      logger.info("Different approach")
    } else {
      if (payload.OPERATION === 'INSERT') {
        await migratepg.migratepgInsert(pool, payload)
          .catch(err => {
            postgreErr = err
          })
      } else if (payload.OPERATION === 'UPDATE') {
        await migratepg.migratepgUpdate(pool, payload)
          .catch(err => {
            postgreErr = err
          })
      } else if (payload.OPERATION === 'DELETE') {
        await migratepg.migratepgDelete(pool, payload)
          .catch(err => {
            postgreErr = err
          })
      }
    }
    //audit success log
    if (!postgreErr) {
      await app_log.consumerpg_success_log(payload)
      return consumer.commitOffset({
        topic: topic,
        partition: partition,
        offset: m.offset,
        metadata: 'optional'
      })
    } else {

      //audit failure log
      logger.logFullError(postgreErr)
      await app_log.consumerpg_failure_log(payload, postgreErr)
      let msgValue = {
        ...postgreErr,
        recipients: config.topic_error.EMAIL,
        payloadposted: JSON.stringify(payload),
        msgoriginator: "consumer-producer"
      }
      let reconcile_flag = payload['RECONCILE_STATUS'] ? payload['RECONCILE_STATUS'] : 0
      if (reconcile_flag != 0) {
        logger.debug('Reconcile failed, sending it to error queue: ', config.topic_error.NAME);
        kafka_error = await pushToKafka(producer, config.topic_error.NAME, msgValue)
        if (!kafka_error) {
          logger.info("Kafka Message posted successfully to the topic : " + config.topic_error.NAME)
        } else {
          if (config.SLACK.SLACKNOTIFY === 'true') {
            await slack.postMessage("consumer_reconcile post fails - unable to post the error in kafka failure topic due to some errors", async (response) => {
              await slack.validateMsgPosted(response.statusCode, response.statusMessage)
            });
          }
        }
        return consumer.commitOffset({
          topic: topic,
          partition: partition,
          offset: m.offset,
          metadata: 'optional'
        })
      }
      if (!payload.retryCount) {
        payload.retryCount = 0
        logger.debug('setting retry counter to 0 and max try count is : ', config.KAFKA_REPOST_COUNT);
      }
      if (payload.retryCount >= config.KAFKA_REPOST_COUNT) {
        logger.debug('Reached at max retry counter, sending it to error queue: ', config.topic_error.NAME);
        kafka_error = await pushToKafka(producer, config.topic_error.NAME, msgValue)
        if (!kafka_error) {
          logger.info("Kafka Message posted successfully to the topic : " + config.topic_error.NAME)
        } else {
          if (config.SLACK.SLACKNOTIFY === 'true') {
            await slack.postMessage("Consumer Retry reached Max- But unable to post kafka due to errors", async (response) => {
              await slack.validateMsgPosted(response.statusCode, response.statusMessage)
            });
          }
        }
      } else {
        //moved to consumerretry function
        await consumerretry(producer, payload)
      }
      return consumer.commitOffset({
        topic: topic,
        partition: partition,
        offset: m.offset,
        metadata: 'optional'
      })

    }
  }).catch(err => logger.logFullError(err))

};

const check = function () {
  if (!consumer.client.initialBrokers && !consumer.client.initialBrokers.length) {
    return false;
  }
  let connected = true;
  consumer.client.initialBrokers.forEach(conn => {
    logger.debug(`url ${conn.server()} - connected=${conn.connected}`);
    connected = conn.connected & connected;
  });
  return connected;
};
/**
 * Initialize kafka consumer
 */
async function setupKafkaConsumer() {
  try {
    const strategies = [{
      subscriptions: [config.topic.NAME],
      handler: dataHandler
    }];
    await consumer.init(strategies);
    logger.info('Initialized kafka consumer')
    healthcheck.init([check])
  } catch (err) {
    logger.error('Could not setup kafka consumer')
    logger.logFullError(err)
    terminate()
  }
}

setupKafkaConsumer()