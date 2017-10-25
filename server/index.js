const express = require('express');
const db = require('../db/index.js')
// var amqp = require('amqplib/callback_api'); // message bus
const request = require('request-promise');
const redis = require('./redisHelper.js');
const bodyParser = require('body-parser');

// amqp.connect(process.env.CLOUDAMQP_URL)

const app = express();

app.use(bodyParser.json());

// db._getZipcodeId(94102)
// .then(data => {
//   console.log('Data:', data);
// })

db.getFireIncidentsByParamsFromDb(94102, "2017-07-25T00:00:00.000", "2017-10-25T00:00:00.000", 'month')
.then(data => {
  if (data && data.length > 0) {
    console.log('Got Data from DB, sending and then caching', data);
    // res.status(200).send(data);
    if (data) {
      console.log('About to cache', /*req.query*/);
      // redis.addToCache(req.query, data, null);
    }
  } else {
    console.log("WHY IS THIS DATA MISSING?", data);
    // res.status(400).send('Outside of boundary');
  }
  console.log('Done');
})
.catch(err => {
  console.error('Error:', err);
  // res.status(500).send(err);
});

app.get('/:params', (req, res) => {
  console.log('Parameters:', req.query);
  const {zipcode, startDate, endDate, granularity} = req.query;

  redis.getFromCache(req.query)
  .then(reply => {
    if (reply) {
      console.log('Found in cache:', reply);
      res.status(200).send(reply);
    } else {
      console.log('Getting data from DB');
      // db._getZipcodeId(zipcode)
      // .then(zipId => {
      db.getFireIncidentsByParamsFromDb(zipcode, startDate, endDate, granularity)
      .then(data => {
        if (data && data.length > 0) {
          console.log('Got Data from DB, sending and then caching');
          res.status(200).send(data);
          if (data) {
            console.log('About to cache', req.query);
            redis.addToCache(req.query, data, null);
          }
        } else {
          console.log("WHY IS THIS DATA MISSING?", data);
          res.status(400).send('Outside of boundary');
        }
        console.log('Done');
      })
      .catch(err => {
        console.error('Error:', err);
        res.status(500).send(err);
      });
      // })
    }
  });

});



app.listen(process.env.PORT || 3000, () => {
  console.log('API server for Fire Instance is LIVE at port:', process.env.PORT || 3000);
});
