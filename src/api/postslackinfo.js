const config = require('config');
const zlib = require('zlib');
const url = require('url');
const https = require('https');
const logger = require('../common/logger')
hookUrl = config.SLACK.URL
slackChannel = config.SLACK.SLACKCHANNEL

async function postMessage(message, callback) {
    var slackMessage = {
        channel: `${slackChannel}`,
        text: `${message}`,
    }
    logger.debug("stringfied slack message");
    logger.debug(JSON.stringify(slackMessage))
    logger.debug("slack message");
    logger.debug(slackMessage)
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
async function validateMsgPosted(responsecode, responsemsg) {
    if (responsecode < 400) {
        logger.info('Message posted successfully');
    } else if (responsecode < 500) {
        logger.logFullError(`Error posting message to Slack API: ${responsecode} - ${responsemsg}`);
    } else {
        logger.logFullError(`Server error when processing message: ${responsecode} - ${responsemsg}`);
    }
}

async function send_msg_to_slack(msg) {
    if (config.SLACK.SLACKNOTIFY === 'true') {
        await postMessage(msg, async (response) => {
            await validateMsgPosted(response.statusCode, response.statusMessage)
        })
    }
}

module.exports = {
    postMessage,
    validateMsgPosted,
    send_msg_to_slack
}