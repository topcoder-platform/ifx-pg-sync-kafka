const express = require('express')
const bodyParser = require('body-parser')
const pushToDynamoDb = require('./api/migratedynamodb')
const config = require('config');

const app = express()
const port = process.env.PORT || 8080;
app.use(bodyParser.json());       // to support JSON-encoded bodies
app.use(bodyParser.urlencoded({     // to support URL-encoded bodies
  extended: true
}));
app.get('/', function (req, res) {
  res.send('hello world')
})

app.post('/fileevents', async function (req, res) {
  const payload = req.body
  console.log({
    topic: config.topic.NAME,
    partition: config.topic.PARTITION,
    message: {
       value : JSON.stringify(payload)
    }
  });
  await pushToDynamoDb(payload);
    res.send('done');

})

app.listen(port);
console.log('Server started! At http://localhost:' + port);
