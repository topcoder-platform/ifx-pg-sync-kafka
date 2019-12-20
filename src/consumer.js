const Kafka = require('no-kafka');
const Promise = require('bluebird');
const config = require('config');
const logger = require('./common/logger');
const consumer = new Kafka.GroupConsumer();
const {
  producerLog,
  pAuditLog
} = require('./api/audit')
const {
  consumerLog,
  cAuditLog
} = require('./api/audit')
//const { migrateDelete, migrateInsert, migrateUpdate } = require('./api/migrate')
const {
  migratepgDelete,
  migratepgInsert,
  migratepgUpdate
} = require('./api/migratepg')
const {
  migrateifxinsertdata,
  migrateifxupdatedata
} = require('./api/migrateifxpg')
const pushToKafka = require('./api/pushToKafka')
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

const dataHandler = function (messageSet, topic, partition) {
  return Promise.each(messageSet, async function (m) {
    const payload = JSON.parse(m.message.value)

    // insert consumer_log
    try
    {
    console.log("payload sequece ID : " + payload.SEQ_ID )
    consumerLog({
        SEQ_ID: payload.SEQ_ID,
        TOPICNAME: topic,
        SCHEMA_NAME: payload.SCHEMANAME,
        CONSUMAER_QUERY: {
          OPERATION: payload.OPERATION,
          DATA: payload.DATA
        },
        DESTINATION: config.DESTINATION
      }).then(log => console.log('Add Consumer Log'))
      .catch(err => console.log(err))
    } catch(error) {console.log(error)}
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
      console.log("Different approach")
    } else {
      if (payload.OPERATION === 'INSERT') {
        let entity = payload.DATA
        await migratepgInsert(pool, entity, payload.SCHEMANAME, payload.TABLENAME)
          .catch(err => {
            postgreErr = err
            //console.log(err)
          })

      } else if (payload.OPERATION === 'UPDATE') {
        await migratepgUpdate(pool, payload.DATA, payload.SCHEMANAME, payload.TABLENAME)
          .catch(err => {
            postgreErr = err
            //console.log(err)
          })

      } else if (payload.OPERATION === 'DELETE') {
        let entity = payload.DATA
        await migratepgDelete(pool, entity, payload.SCHEMANAME, payload.TABLENAME)
          .catch(err => {
            postgreErr = err
            //console.log(err)
          })

      }
    }
    //audit success log
    if (!postgreErr) {
      await cAuditLog({
          SEQ_ID: payload.SEQ_ID,
          CONSUMER_DEPLOY_STATUS: 'success',
          CONSUMER_UPDATE_TIME: Date.now()
        }).then(log => console.log('postgres ' + payload.OPERATION + ' success'))
        .catch(err => console.log(err))

      return consumer.commitOffset({
        topic: topic,
        partition: partition,
        offset: m.offset,
        metadata: 'optional'
      })
    } else {

      //audit failure log
      await cAuditLog({
          SEQ_ID: payload.SEQ_ID,
          CONSUMER_DEPLOY_STATUS: 'failure',
          CONSUMER_FAILURE_LOG: postgreErr,
          CONSUMER_UPDATE_TIME: Date.now()
        }).then((log) => console.log('postgres ' + payload.OPERATION + ' failure'))
        .catch(err => console.log(err))

      let msgValue = {
        ...postgreErr,
        recipients: config.topic_error.EMAIL,
        payloadposted: JSON.stringify(payload)
      }

      if (!payload.retryCount) {
        payload.retryCount = 0
        logger.debug('setting retry counter to 0 and max try count is : ', config.KAFKA_REPOST_COUNT);
      }
      if (payload.retryCount >= config.KAFKA_REPOST_COUNT) {
        logger.debug('Recached at max retry counter, sending it to error queue: ', config.topic_error.NAME);
        kafka_error = await pushToKafka(producer, config.topic_error.NAME, msgValue)

      } else {
        payload['retryCount'] = payload.retryCount + 1;
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

        if (!seqID) {
          console.log('ProducerLog Failure')
          return
        }
        console.log('ProducerLog Success')
        payload['SEQ_ID'] = seqID;
        //SEQ_ID: seqID
        kafka_error = await pushToKafka(producer, config.topic.NAME, payload)
        //add auditlog
        if (!kafka_error) {
          await pAuditLog({
            SEQ_ID: seqID,
            PRODUCER_PUBLISH_STATUS: 'success',
            PRODUCER_PUBLISH_TIME: Date.now()
          }).then((log) => console.log('Send Success'))
          //res.send('done')
          return consumer.commitOffset({
            topic: topic,
            partition: partition,
            offset: m.offset,
            metadata: 'optional'
          })
        } else {
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
            recipients: config.topic_error.EMAIL,
            msgoriginator: "consumer-producer"
          }
          //send error message to kafka
          kafka_error = await pushToKafka(producer, config.topic_error.NAME, msgValue)
        }
      }
      //send postgres_error message

      //===============================================
      // commit offset
      return consumer.commitOffset({
        topic: topic,
        partition: partition,
        offset: m.offset,
        metadata: 'optional'
      })
    }
  }).catch(err => console.log(err))

};

const strategies = [{
  subscriptions: [config.topic.NAME],
  handler: dataHandler
}];

consumer.init(strategies);