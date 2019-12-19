const Kafka = require('no-kafka');
const Promise = require('bluebird');
const config = require('config');

const consumer = new Kafka.GroupConsumer();
const { consumerLog, cAuditLog } = require('./api/audit')
//const { migrateDelete, migrateInsert, migrateUpdate } = require('./api/migrate')
const { migratepgDelete, migratepgInsert, migratepgUpdate } = require('./api/migratepg')
const { migrateifxinsertdata,migrateifxupdatedata } = require('./api/migrateifxpg')
//const { migrateinsertdata } =  require('./api/migrate-data')
const producer = new Kafka.Producer()

producer.init().then(function () {
  console.log('connected to local kafka server on port 9092 ...');
}
).catch(e => { console.log('Error : ', e) });

const {
  createPool,
} = require('./common/postgresWrapper');
database=config.get('POSTGRES.database');
const pool = createPool(database);
pool.on('remove', client => {
console.log("setting property to on query completion");
})
console.log('---------------------------------');

const dataHandler = function (messageSet, topic, partition) {
    return Promise.each(messageSet, async function (m) {
      const payload = JSON.parse(m.message.value)

      // insert consumer_log
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

      //update postgres table
      let postgreErr
      if(payload.uniquedatatype === 'true') {
        //retrive teh data from info and insert in postgres
        console.log("welcome")
        //await migrateinsertdata(payload, pool)
        console.log(pool);
        if(payload.OPERATION === 'INSERT') {
            await migrateifxinsertdata(payload, pool)
            .catch(err => {
              postgreErr = err
              console.log(err)
            })            
         }
        if(payload.OPERATION === 'UPDATE') {
            await migrateifxupdatedata(payload, pool)
            .catch(err => {
              postgreErr = err
              console.log(err)
            })            
         }
        console.log("Different approach")
      }
      else
      {
      if(payload.OPERATION === 'INSERT') {
        let entity = payload.DATA
        await migratepgInsert(pool, entity, payload.SCHEMANAME, payload.TABLENAME)
          .catch(err => {
            postgreErr = err
            console.log(err)
          })

      } else if(payload.OPERATION === 'UPDATE') {
        await migratepgUpdate(pool, payload.DATA, payload.SCHEMANAME, payload.TABLENAME)
          .catch(err => {
            postgreErr = err
            console.log(err)
          })

      } else if(payload.OPERATION === 'DELETE') {
        let entity = payload.DATA
        await migratepgDelete(pool, entity, payload.SCHEMANAME, payload.TABLENAME)
          .catch(err => {
            postgreErr = err
            console.log(err)
          })

      }
    }
      //audit success log
      if(!postgreErr){
      await cAuditLog({
        SEQ_ID: payload.SEQ_ID,
        CONSUMER_DEPLOY_STATUS: 'success',
        CONSUMER_UPDATE_TIME: Date.now()
      }).then(log => console.log('postgres '+ payload.OPERATION + ' success'))
        .catch(err => console.log(err))

      return consumer.commitOffset({ topic: topic, partition: partition, offset: m.offset, metadata: 'optional' })
      }

//================      

      //audit failure log
      await cAuditLog({
        SEQ_ID: payload.SEQ_ID,
        CONSUMER_DEPLOY_STATUS: 'failure',
        CONSUMER_FAILURE_LOG: postgreErr,
        CONSUMER_UPDATE_TIME: Date.now()
      }).then((log) => console.log('postgres '+ payload.OPERATION + ' failure'))
        .catch(err => console.log(err))

      let msgValue = {
        ...postgreErr,
        recipients: config.topic_error.EMAIL,
        payloadposted: JSON.stringify(payload)
      }

//===================================

      if (!payload.retryCount) {
        payload.retryCount = 0
        logger.debug('setting retry counter to 0 and max try count is : ', config.KAFKA_REPOST_COUNT);
      }
      if (payload.retryCount >= config.KAFKA_REPOST_COUNT) {
        logger.debug('Recached at max retry counter, sending it to error queue: ', config.topic_error.NAME);
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
      }
      else
      {
         payload['retryCount'] = payload.retryCount + 1;

         await producer.send({
          topic: config.topic.NAME,
          partition: config.topic.PARTITION,
          message: {
            value : JSON.stringify(payload)
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

            console.log(kafka_error)  
        })
      
 //await auditTrail([message.payload.payloadseqid,cs_processId,message.payload.table,message.payload.Uniquecolumn,
   //       message.payload.operation,"Error",message.payload['retryCount'],err.message,"",message.payload.data, message.timestamp,message.topic],'consumer')
     // await pushToKafka(message)      
      }
      //send postgres_error message

//===============================================
      // commit offset
      return consumer.commitOffset({ topic: topic, partition: partition, offset: m.offset, metadata: 'optional' })
    }).catch(err => console.log(err))
};

const strategies = [{
    subscriptions: [config.topic.NAME],
    handler: dataHandler
}];

consumer.init(strategies);
