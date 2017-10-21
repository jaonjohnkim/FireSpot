var Cron = require('cron').CronJob;
var request = require('request');
var worker = require('worker.js');

var job = new Cron({
  cronTime: '00 57 23 * * *', // Run at 11:57PM every day
  onTick: worker.start(),
  start: true,
  timeZone: "America/Los_Angeles"
});

job.start();
console.log('Fire Instance Clock Job Status:', job.running);
