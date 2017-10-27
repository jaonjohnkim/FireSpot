const express = require('express');
const db = require('../db/index.js')
// var amqp = require('amqplib/callback_api'); // message bus
const request = require('request-promise');
const redis = require('./redisHelper.js');
const bodyParser = require('body-parser');
const statsD = require('node-statsd');
const statsDClient = new statsD({
  host: 'statsd.hostedgraphite.com',
  port: 8125,
  prefix: '00436c17-5dfb-4df2-bd21-634d9a0ab64f'
});

// amqp.connect(process.env.CLOUDAMQP_URL)

const app = express();

// app.use(bodyParser.json());

// db._getZipcodeId(94102)
// .then(data => {
//   console.log('Data:', data);
// })

// db.getFireIncidentsByParamsFromDb(94102, "2017-07-25T00:00:00.000", "2017-10-25T00:00:00.000", 'month')
// .then(data => {
//   if (data && data.length > 0) {
//     console.log('Got Data from DB, sending and then caching', data);
//     // res.status(200).send(data);
//     if (data) {
//       console.log('About to cache', /*req.query*/);
//       // redis.addToCache(req.query, data, null);
//     }
//   } else {
//     console.log("WHY IS THIS DATA MISSING?", data);
//     // res.status(400).send('Outside of boundary');
//   }
//   console.log('Done');
// })
// .catch(err => {
//   console.error('Error:', err);
//   // res.status(500).send(err);
// });

app.get('/:params', async (req, res) => {
  const start = Date.now();
  const {zipcode, granularity} = req.query;
  let {startDate, endDate} = req.query;
  const today = db.stringifyDate(new Date());
  let threeMonthsAgo = new Date();
  threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);
  const threeMonthsAgoStr = db.stringifyDate(threeMonthsAgo);
  startDate = startDate || threeMonthsAgoStr;
  endDate = endDate || today;
  const reply = await redis.getFromCache(req.query)
  if (reply) {
    console.log('Found in cache:', reply);
    res.status(200).send(JSON.parse(reply));
    const latency = Date.now() - start;
    statsDClient.histogram('query.latency_ms', latency);
    statsDClient.increment('query.cache.count');
  } else {
    console.log('Not found in cache, getting data from DB');
    db.getFireIncidentsByParamsFromDb(zipcode, startDate, endDate, granularity)
    .then(data => {
      if (data && data.length > 0) {
        console.log('Got Data from DB, sending and then caching');
        res.status(200).send(data);
        const latency = Date.now() - start;
        statsDClient.histogram('query.latency_ms', latency);
        statsDClient.increment('query.db.count');
        if (data) {
          console.log('About to cache', req.query);
          redis.addToCache(req.query, data, null);
        }
      } else {
        res.status(400).send('Outside of boundary');
        const latency = Date.now() - start;
        statsDClient.histogram('query.latency_ms', latency);
        statsDClient.increment('query.fail');
      }
      console.log('Done');
    })
    .catch(err => {
      console.error('Error:', err);
      res.status(500).send(err);
      const latency = Date.now() - start;
      statsDClient.histogram('query.latency_ms', latency);
      statsDClient.increment('query.fail');
    });
  }
});



app.listen(process.env.PORT || 3000, () => {
  console.log('API server for Fire Instance is LIVE at port:', process.env.PORT || 3000);
});
