const config = require('config');
const zlib = require('zlib');
const url = require('url');
const https = require('https');
hookUrl = config.SLACK.URL
slackChannel = config.SLACK.SLACKCHANNEL

function postMessage(message, callback) {

    var slackMessage = {
        channel: `${slackChannel}`,
        text: `${message}`,
    }
    console.log("stringfied slack message");
    console.log(JSON.stringify(slackMessage))
    console.log("slack message");
    console.log(slackMessage)
    const body = JSON.stringify(slackMessage);
    const options = url.parse(hookUrl);
    options.method = 'POST';
    options.headers = {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
    };

    const postReq = https.request(options, (res) => {
        const chunks = [];
        res.setEncoding('utf8');
        res.on('data', (chunk) => chunks.push(chunk));
        res.on('end', () => {
            if (callback) {
                callback({
                    body: chunks.join(''),
                    statusCode: res.statusCode,
                    statusMessage: res.statusMessage,
                });
            }
        });
        return res;
    });

    postReq.write(body);
    postReq.end();
}
function validateMsgPosted(responsecode,responsemsg) {
    if (responsecode < 400) {
        console.info('Message posted successfully');
      } else if (responsecode < 500) {
        console.error(`Error posting message to Slack API: ${responsecode} - ${responsemsg}`);
      } else {
        console.log(`Server error when processing message: ${responsecode} - ${responsemsg}`);
      }
}

module.exports = { 
    postMessage,
    validateMsgPosted
}