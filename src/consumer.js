const Kafka = require('no-kafka');
const Promise = require('bluebird');
const config = require('config');
const logger = require('./common/logger');
const healthcheck = require('topcoder-healthcheck-dropin');
const consumer = new Kafka.GroupConsumer();
const {
  create_consumer_app_log,
  consumerpg_success_log,
  consumerpg_failure_log
} = require('./common/app_log')
//const { migrateDelete, migrateInsert, migrateUpdate } = require('./api/migrate')
const {
  migratepgDelete,
  migratepgInsert,
  migratepgUpdate
} = require('./api/migratepg')
const {
  migrateifxinsertdata,
  migrateifxupdatedata,
  migrateifxdeletedata
} = require('./api/migrateifxpg')
const pushToKafka = require('./api/pushToKafka')
const {
  postMessage,
  validateMsgPosted
} = require('./api/postslackinfo')
const consumerretry = require('./api/consumer_retry')
//const { migrateinsertdata } =  require('./api/migrate-data')
const producer = new Kafka.Producer()

producer.init().then(function () {
  console.log('connected to local kafka server on port 9092 ...');
}).catch(e => {
  console.log('Error : ', e)
});

const {
  createPool,
} = require('./common/postgresWrapper');
database = config.get('POSTGRES.database');
const pool = createPool(database);
pool.on('remove', client => {
  console.log("setting property to on query completion");
})
console.log('---------------------------------');

async function dataHandler(messageSet, topic, partition) {
  return Promise.each(messageSet, async function (m) {
    const payload = JSON.parse(m.message.value)

    // insert consumer_log
    try {
      await create_consumer_app_log(payload)
    } catch (error) {
      console.log(error)
    }

    //update postgres table
    let postgreErr
    if (payload.uniquedatatype === 'true') {
      //retrive teh data from info and insert in postgres
      console.log("welcome")
      //await migrateinsertdata(payload, pool)
      console.log(pool);
      if (payload.OPERATION === 'INSERT') {
        await migrateifxinsertdata(payload, pool)
          .catch(err => {
            postgreErr = err
            //console.log(err)
          })
      }
      if (payload.OPERATION === 'UPDATE') {
        await migrateifxupdatedata(payload, pool)
          .catch(err => {
            postgreErr = err
            //console.log(err)
          })
      }
      if (payload.OPERATION === 'DELETE') {
        await migrateifxdeletedata(payload, pool)
          .catch(err => {
            postgreErr = err
            //console.log(err)
          })
      }

      console.log("Different approach")
    } else {
      if (payload.OPERATION === 'INSERT') {
        let entity = payload.DATA
        await migratepgInsert(pool, payload)
          .catch(err => {
            postgreErr = err
            //console.log(err)
          })

      } else if (payload.OPERATION === 'UPDATE') {
        await migratepgUpdate(pool, payload)
          .catch(err => {
            postgreErr = err
            //console.log(err)
          })

      } else if (payload.OPERATION === 'DELETE') {
        let entity = payload.DATA
        await migratepgDelete(pool, payload)
          .catch(err => {
            postgreErr = err
            //console.log(err)
          })

      }
    }
    //audit success log
    if (!postgreErr) {
      await consumerpg_success_log(payload)
      return consumer.commitOffset({
        topic: topic,
        partition: partition,
        offset: m.offset,
        metadata: 'optional'
      })
    } else {

      //audit failure log
      console.log(postgreErr)
      await consumerpg_failure_log(payload, postgreErr)
      
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
          console.log("Kafka Message posted successfully to the topic : " + config.topic_error.NAME)
        } else {
          if (config.SLACK.SLACKNOTIFY === 'true') {
            await postMessage("consumer_reconcile post fails - unable to post the error in kafka failure topic due to some errors", async (response) => {
              await validateMsgPosted(response.statusCode, response.statusMessage)
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
          console.log("Kafka Message posted successfully to the topic : " + config.topic_error.NAME)
        } else {
          if (config.SLACK.SLACKNOTIFY === 'true') {
            await postMessage("Consumer Retry reached Max- But unable to post kafka due to errors", async (response) => {
              await validateMsgPosted(response.statusCode, response.statusMessage)
            });
          }
        }


      } else {
//moved to consumerretry function
        await consumerretry(producer,payload)
      }
      return consumer.commitOffset({
        topic: topic,
        partition: partition,
        offset: m.offset,
        metadata: 'optional'
      })

    }
  }).catch(err => console.log(err))

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