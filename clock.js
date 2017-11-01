const Cron = require('cron').CronJob;
const request = require('request');
const worker = require('./worker.js');


const job = new Cron({
  cronTime: '00 59 23 * * *', // Run at 11:59PM every day
  onTick: worker.start(),
  start: true,
  timeZone: "America/Los_Angeles"
});


job.start();
console.log('Fire Instance Clock Job Status:', job.running);
