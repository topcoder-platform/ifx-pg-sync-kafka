const Kafka = require('no-kafka');
const Promise = require('bluebird');
const config = require('config');
const slack = require('./api/postslackinfo')
const consumer = new Kafka.GroupConsumer();

const dataHandler = function (messageSet, topic, partition) {
    return Promise.each(messageSet, async function (m) {
      const payload = JSON.parse(m.message.value)
      if(config.SLACK.SLACKNOTIFY === 'true') {
        console.log(payload)
        await slack.postMessage(Object.values(payload), async (response) => {
            await slack.validateMsgPosted(response.statusCode, response.statusMessage)
        });
        }
     
      // commit offset
     consumer.commitOffset({ topic: topic, partition: partition, offset: m.offset, metadata: 'optional' })
    }).catch(err => console.log(err))
};

const strategies = [{
    subscriptions: [config.topic_error.NAME],
    handler: dataHandler
}];

consumer.init(strategies);
