const express = require('express')
const bodyParser = require('body-parser')
const {pushToDynamoDb} = require('./api/migratedynamodb')

const app = express()
const port = process.env.PORT || 8080;
app.use(bodyParser.json());       // to support JSON-encoded bodies
app.use(bodyParser.urlencoded({     // to support URL-encoded bodies
  extended: true
}));
app.get('/', function (req, res) {
  res.send('hello world')
})

app.post('/fileevents', function (req, res) {
  const payload = req.body
  // const topic = payload.topic
  const topic = 'test-topic';
  console.log({
    topic: config.topic.NAME,
    partition: config.topic.PARTITION,
    message: {
       value : JSON.stringify(payload)
    }
  });
  await pushToDynamoDb(payload);
    res.send('done');

  // send response to client 
  //res.send('ok')

})

// const producer = new Kafka.Producer()

// producer.init().then(function () {
//   console.log('connected to local kafka server on port 9092 ...');

//   // start the server
//   app.listen(port);
//   console.log('Server started! At http://localhost:' + port);

// } //end producer init
// ).catch(e => { console.log('Error : ', e) });

app.listen(port);
console.log('Server started! At http://localhost:' + port);
