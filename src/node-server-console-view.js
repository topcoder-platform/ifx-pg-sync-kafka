const express = require('express')
const bodyParser = require('body-parser')

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
    topic: topic,
    partition: 0,
    message: {
       value : JSON.stringify(payload)
    }
  });
    res.send('done');

  // send response to client 
  //res.send('ok')

})


app.listen(port);
console.log('Server started! At http://localhost:' + port);
