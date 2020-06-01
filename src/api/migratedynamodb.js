const config = require('config')
const logger = require('../common/logger')
const _ = require('lodash')
var AWS = require("aws-sdk");
async function pushToDynamoDb(payload) {
  try {
    logger.debug('----Inside DynomoDB code -------');
    let seqID = payload.TIME + "_" + payload.TABLENAME
    var params = {
      TableName: config.DYNAMODB.TABLENAME,
      Item: {
        SequenceID: seqID,
        pl_time: payload.TIME,
        pl_document: payload,
        pl_table: payload.TABLENAME,
        pl_schemaname: payload.SCHEMANAME,
        pl_operation: payload.OPERATION,
        pl_uniquedatatype: payload.uniquedatatype,
        NodeSequenceID: Date.now()
      }
    }
    var docClient = new AWS.DynamoDB.DocumentClient({
      region: config.DYNAMODB.REGION,
      convertEmptyValues: true
    });
    docClient.put(params, function (err, data) {
      if (err) {
        logger.error('DynamoDB error : ')
        logger.logFullError(err);
      } else {
        logger.info('DynamoDB Success : ', data);
      }
    });

  } catch (e) {
    logger.logFullError(e);
  }
}
module.exports = pushToDynamoDb