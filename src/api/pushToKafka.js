/*
 * Kafka producer that sends messages to Kafka server.
 */
const config = require('config')
const logger = require('../common/logger')
const _ = require('lodash')

async function pushToKafka(producer, topicname, payload) {
    let kafka_error     
    await producer.send({
        topic: topicname,
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
            {
            kafka_error = result[0].error
            console.log(kafka_error)  
            return kafka_error
            }
            console.log(result)
      })    
    return
}

module.exports = pushToKafka