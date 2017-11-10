const express = require('express');
const db = require('../db/index.js')
// var amqp = require('amqplib/callback_api'); // message bus
const os = require('os');
const osUtil = require('os-utils');
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
  osUtil.cpuUsage((v) => {
    statsDClient.gauge('.service.health.cpu.percent', v);
  })
  statsDClient.gauge('.service.health.memory.used.percent', (os.totalmem() - os.freemem() / os.totalmem()));
  statsDClient.gauge('.service.health.memory.used.bytes', os.totalmem() - os.freemem());
  statsDClient.gauge('.service.health.memory.free.bytes', os.freemem());

  statsDClient.increment('.service.health.query.all');
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
  const reqObj = {
    zipcode: zipcode,
    startDate: startDate,
    endDate: endDate,
    granularity: granularity
  };
  // console.log("startDate:", startDate, typeof startDate);
  // console.log("endDate:", endDate, typeof endDate);
  const reply = await redis.getFromCache(reqObj);
  if (reply) {
    res.status(200).send(JSON.parse(reply));
    statsDClient.timing('.service.health.query.latency_ms', Date.now() - start);
    statsDClient.increment('.service.health.query.cache');
  } else {
    console.log('Not found in cache, getting data from DB');
    // console.log("startDate:", startDate, typeof startDate);
    // console.log("endDate:", endDate, typeof endDate);
    db.getFireIncidentsByParamsFromDb(zipcode, startDate, endDate, granularity)
    .then(data => {
      if (data && data.length > 0) {
        console.log('Got Data from DB, sending and then caching');
        res.status(200).send(data);
        statsDClient.timing('.service.health.query.latency_ms', Date.now() - start);
        statsDClient.increment('.service.health.query.db');
        if (data) {
          // console.log('About to cache', req.query);
          redis.addToCache(reqObj, data, null);
        }
      } else {
        res.status(400).send('Outside of boundary');
        const latency = Date.now() - start;
        statsDClient.timing('.service.health.query.latency_ms', Date.now() - start);
        statsDClient.increment('.service.health.query.fail');
      }
      // console.log('Done');
    })
    .catch(err => {
      console.error('Error:', err);
      res.status(500).send(err);
      const latency = Date.now() - start;
      statsDClient.timing('.service.health.query.latency_ms', Date.now() - start);
      statsDClient.increment('.service.health.query.fail');
    });
  }
});

app.listen(process.env.PORT || 3000, () => {
  console.log('API server for Fire Instance is LIVE at port:', process.env.PORT || 3000 );
});
