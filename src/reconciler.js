const config = require('config')
//Establishing connection in postgress
const pg = require('pg')
const pgOptions = config.get('POSTGRES')
const database = 'auditlog'
const pgConnectionString = `postgresql://${pgOptions.user}:${pgOptions.password}@${pgOptions.host}:${pgOptions.port}/${database}`
console.log(pgConnectionString)
const pgClient = new pg.Client(pgConnectionString)
//pgClient.connect();i
async function setupPgClient () {
  try {
    await pgClient.connect()
	logger.debug('Connected to Pg Client2 Audit:')
    }
   catch (err) {
    logger.error('Could not setup postgres client2')
    logger.logFullError(err)
    process.exit()
  }
}
const logger = require('./src/common/logger')
const _ = require('lodash')
var AWS = require("aws-sdk");

async function dynamo_pg_validation()
{
var docClient = new AWS.DynamoDB.DocumentClient({
    region: config.DYNAMODB.REGION,
    convertEmptyValues: true
  });

//ElapsedTime = 600000
ElapsedTime = 4995999000
  var params = {
    TableName: config.DYNAMODB.TABLENAME,
    FilterExpression: "NodeSequenceID between :time_1 and :time_2",
    ExpressionAttributeValues: {
        ":time_1": Date.now() - ElapsedTime,
        ":time_2": Date.now()
    }
  }
  console.log("scanning");
  await docClient.scan(params, onScan);
}
async function onScan(err, data) {
    if (err) {
        console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
    } else {

        console.log("Scan succeeded.");
        data.Items.forEach(async function(item) {
//            console.log(item);
            await validate_data_in_pg(item.SequenceID,item.pl_document)

        });

        // continue scanning if we have more movies, because
        // scan can retrieve a maximum of 1MB of data

        if (typeof data.LastEvaluatedKey != "undefined") {
            console.log("Scanning for more...");
            params.ExclusiveStartKey = data.LastEvaluatedKey;
            await docClient.scan(params, onScan);
        }
    }
}

async function validate_data_in_pg(SequenceID,payload)
{
    console.log(SequenceID);
    let schemaname = 'public';     
    const sqlquerytovalidate = 'SELECT COUNT(*) FROM audit_log WHERE seq_id=$1';
    const sqlquerytovalidate_values =[SequenceID]
    console.log(sqlquerytovalidate);
    await pgClient.query(sqlquerytovalidate, sqlquerytovalidate_values,async (err,res) => {
   
             if (err) {
        var errmsg0 = `error-sync: Audit reconsiler query  "${err.message}"`
        logger.debug (errmsg0)
        // await callposttoslack(errmsg0)
}
else
{
      console.log("validating data count---------------------");
      const data = res.rows;
      data.forEach(async(row) => { 
          if (row['count'] == 0  )
          {
            //await posttopic(payload,0)
            console.log("post the topic");
          }
          else
          {
            console.log(`${SequenceID} is exist in pg`)
          }

          
    });
}
    });
}
async function main()
{
await setupPgClient()
await dynamo_pg_validation()
await pgClient.end()
}
main()
// docClient.query(params, function(err, data) {
//     if (err) {
//         console.error("Unable to query. Error:", JSON.stringify(err, null, 2));
//     } else {
//         console.log("Query succeeded.");
//         data.Items.forEach(function(item) {
//             console.log(" -", item.year + ": " + item.title);
//         //select query from for last 10 mins pg fethte seq_id
//         //compare the dynamo seqid exist with pgseqid
//         //if not exist , post res api with paylaod from dynamodb            
//         });
//     }
// });
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