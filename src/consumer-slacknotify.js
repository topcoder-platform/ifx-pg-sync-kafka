const Kafka = require('no-kafka');
const Promise = require('bluebird');
const config = require('config');
const postMessage = require('./api/postslackinfo');
const consumer = new Kafka.GroupConsumer();

const dataHandler = function (messageSet, topic, partition) {
    return Promise.each(messageSet, async function (m) {
      const payload = JSON.parse(m.message.value)
      if(config.SLACK.SLACKNOTIFY === 'true') {
        console.log(payload)
        postMessage(payload, (response) => {
            if (response.statusCode < 400) {
                console.info('Message posted successfully');
              //  callback(null);
            } else if (response.statusCode < 500) {
                console.error(`Error posting message to Slack API: ${response.statusCode} - ${response.statusMessage}`);
               // callback(null);  // Don't retry because the error is due to a problem with the request
            } else {
                // Let Lambda retry
                console.log(`Server error when processing message: ${response.statusCode} - ${response.statusMessage}`);
                //callback(`Server error when processing message: ${response.statusCode} - ${response.statusMessage}`);
            }
        });
        }
     
      // commit offset
      return consumer.commitOffset({ topic: topic, partition: partition, offset: m.offset, metadata: 'optional' })
    }).catch(err => console.log(err))
};

const strategies = [{
    subscriptions: [config.topic_error.NAME],
    handler: dataHandler
}];

consumer.init(strategies);
