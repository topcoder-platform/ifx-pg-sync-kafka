const config = require('config')
//Establishing connection in postgress
const pg = require('pg')
const pgOptions = config.get('POSTGRES')
const database = 'auditlog'
const pgConnectionString = `postgresql://${pgOptions.user}:${pgOptions.password}@${pgOptions.host}:${pgOptions.port}/${database}`
const pgClient = new pg.Client(pgConnectionString)

//const auditTrail = require('./services/auditTrail');
//const port = 3000

const logger = require('./common/logger')
const _ = require('lodash')
var AWS = require("aws-sdk");

var docClient = new AWS.DynamoDB.DocumentClient({
    region: config.DYNAMODB.REGION,
    convertEmptyValues: true
  });

//   SequenceID: seqID,
//   pl_time: payload.TIME,
//   pl_document: payload,
//   NodeSequenceID: Date.now()
ElapsedTime = 600000
  var params = {
    TableName: config.DYNAMODB.TABLENAME,
    KeyConditionExpression: "pl_time between :time_1 and :time_2",
    ExpressionAttributeValues: {
        ":time_1": Date.now() - ElapsedTime,
        ":time_2": Date.now()
    }
  }


docClient.query(params, function(err, data) {
    if (err) {
        console.error("Unable to query. Error:", JSON.stringify(err, null, 2));
    } else {
        console.log("Query succeeded.");
        data.Items.forEach(function(item) {
            console.log(" -", item.year + ": " + item.title);
        //select query from for last 10 mins pg fethte seq_id
        //compare the dynamo seqid exist with pgseqid
        //if not exist , post res api with paylaod from dynamodb            
        });
    }
});
//case 1 Reading dequence ID from dynamo DB


// case 1 :

// Dynamo DB only exist . But not in pg

// Get last 10 minutes sequenceid based on payload time pl_time from dynamodb
// Check the sequenceid existance in PG audit auditlog
// If exist, please ignore
// If not exist, retrive the  payload for the respective sequenceid from dynamodb
// Post the payload to producer with restapi

// Case 2:

// Fetch the row with below condition from auditlog

// 1) PAYLOAD_TIME < currenttime -5 and PAYLOAD_TIME > currenttime -25 (This will max time limit will the reconcile logic need to considered)
// 2) OVERALL_STATUS should not be PostgresUpdated (This will help identify which need to be reposted)
// 3) RECONCILE_STATUS is equal to 0  (This will help to set reconcile try count with in particular time schedule)

// Get the payload from PRODUCER_PAYLOAD on audit log for the abiove condition
// Check the RECONCILE_STATUS exist on payload. 
// if not, inititate the RECONCILE_STATUS as 1 and embed in payload
// if exist, increase by 1 and update in payload
// Repost the payload to producer