const config = require('config')
const logger = require('../common/logger')
const _ = require('lodash')
var AWS = require("aws-sdk");
async function pushToDynamoDb(payload) {
  try { console.log('----Inside DynomoDB code -------');
         // console.log(payload)
            var params = {
            TableName: config.DYNAMODB.TABLENAME,
            Item: {
                SequenceID: payload.TIME,
                pl_document: payload,
                pl_table: payload.TABLENAME,
                pl_schemaname: payload.SCHEMANAME,
                pl_operation: payload.OPERATION,
                pl_uniquedatatype: payload.uniquedatatype,
                NodeSequenceID: Date.now()
                }
                }
          var docClient = new AWS.DynamoDB.DocumentClient({region: config.DYNAMODB.Region});
          docClient.put(params, function(err, data) {
        if (err) console.log('DynamoDB error : ', err);
        else console.log('DynamoDB Success : ',data);
        });

  } catch (e) {
          console.log(e)
  }
}
module.exports = pushToDynamoDb