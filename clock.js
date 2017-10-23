var Cron = require('cron').CronJob;
var request = require('request');
var worker = require('./worker.js');


var job = new Cron({
  cronTime: '00 59 23 * * *', // Run at 11:59PM every day
  onTick: worker.start(),
  start: true,
  timeZone: "America/Los_Angeles"
});


job.start();
console.log('Fire Instance Clock Job Status:', job.running);

//Start worker 1st time
worker.start();
