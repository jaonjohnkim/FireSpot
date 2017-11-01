const express = require('express');
const db = require('../db/index.js')
// var amqp = require('amqplib/callback_api'); // message bus
const request = require('request-promise');
const redis = require('./redisHelper.js');
const statsD = require('node-statsd');
const statsDClient = new statsD({
  host: 'statsd.hostedgraphite.com',
  port: 8125,
  prefix: process.env.HOSTEDGRAPHITE_APIKEY
});

// amqp.connect(process.env.CLOUDAMQP_URL)

const app = express();

// const sendMonitorData = ()

app.get('/*', async (req, res) => {
  statsDClient.increment('.service.fire.query.all');
  const start = Date.now();
  let {zipcode, startDate, endDate, granularity} = req.query;

  const today = db.stringifyDate(new Date());
  let threeMonthsAgo = new Date();
  threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);
  const threeMonthsAgoStr = db.stringifyDate(threeMonthsAgo);

  zipcode = zipcode || 94102;
  startDate = startDate || threeMonthsAgoStr;
  endDate = endDate || today;
  granularity = granularity || 'week';

  // console.log("startDate:", startDate, typeof startDate);
  // console.log("endDate:", endDate, typeof endDate);
  console.log('Latency before redis check: ', Date.now() - start);
  const reply = await redis.getFromCache(req.query)
  console.log('Latency after redis check: ', Date.now() - start);
  if (reply) {
    console.log('Found in cache:', reply);
    console.log('Latency before data send: ', Date.now() - start);
    res.status(200).send(JSON.parse(reply));
    console.log('Latency after data send: ', Date.now() - start);
    const latency = Date.now() - start;
    statsDClient.timing('.service.fire.query.latency_ms', latency);
    statsDClient.increment('.service.fire.query.cache');
  } else {
    console.log('Not found in cache, getting data from DB');
    console.log("startDate:", startDate, typeof startDate);
    console.log("endDate:", endDate, typeof endDate);
    db.getFireIncidentsByParamsFromDb(zipcode, startDate, endDate, granularity)
    .then(data => {
      if (data && data.length > 0) {
        console.log('Got Data from DB, sending and then caching');
        console.log('Latency before data send: ', Date.now() - start);
        res.status(200).send(data);
        console.log('Latency after data send: ', Date.now() - start);
        const latency = Date.now() - start;
        statsDClient.timing('.service.fire.query.latency_ms', latency);
        statsDClient.increment('.service.fire.query.db');
        // if (data) {
        //   console.log('About to cache', req.query);
        //   redis.addToCache(req.query, data, null);
        // }
      } else {
        console.log('Latency before data send: ', Date.now() - start);
        res.status(400).send('Outside of boundary');
        console.log('Latency after data send: ', Date.now() - start);
        const latency = Date.now() - start;
        statsDClient.timing('.service.fire.query.latency_ms', latency);
        statsDClient.increment('.service.fire.query.fail');
      }
      console.log('Done');
    })
    .catch(err => {
      console.error('Error:', err);
      console.log('Latency before data send: ', Date.now() - start);
      res.status(500).send(err);
      console.log('Latency after data send: ', Date.now() - start);
      const latency = Date.now() - start;
      statsDClient.timing('.service.fire.query.latency_ms', latency);
      statsDClient.increment('.service.fire.query.fail');
    });
  }
});

app.listen(process.env.PORT || 3001, () => {
  console.log('API server for Fire Instance is LIVE at port:', process.env.PORT || 3001 );
});
