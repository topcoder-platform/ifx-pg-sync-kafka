const config = require('config')
const logger = require('../common/logger')
const _ = require('lodash')
const AWS = require("aws-sdk");
const docClient = new AWS.DynamoDB.DocumentClient({
  region: config.DYNAMODB.REGION,
  convertEmptyValues: true
});
async function pushToDynamoDb(payload) {
  return new Promise(async function (resolve, reject) {
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
      docClient.put(params, function (err, data) {
        if (err) {
          logger.error('DynamoDB update error : ')
          //logger.logFullError(err);
          reject(err)
        } else {
          logger.info('DynamoDB Success : ', data);
          resolve(true);
        }
      });

    } catch (e) {
      logger.error('DynamoDB error : ')
      //logger.logFullError(e);
      reject(e)
    }
  });
}

async function getdata_DynamoDb(uniqueid) {
  return new Promise(async function (resolve, reject) {
    var params = {
      TableName: 'ifxpg-migrator',
      KeyConditionExpression: 'SequenceID = :hkey',
      ExpressionAttributeValues: {
        ':hkey': uniqueid
      }
    };
    docClient.query(params, function (err, data) {
      if (err) {
        reject(err)
      } else {
        resolve(data);
      }
    });
  })
}

module.exports = {
  pushToDynamoDb,
  getdata_DynamoDb
}