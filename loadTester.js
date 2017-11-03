const request = require('request-promise');

setInterval(() => {
  // const zipcodes = [94102, 94111];
  // let zipcode = zipcodes[Math.round(Math.random())];
  zipcode = 94111;
  request(`http://34.238.93.174:8080/json?zipcode=${zipcode}&startDate=2017-07-01T00:00:00.000&endDate=2017-10-25T00:00:00.000&granularity=month`)
  console.log('Pinged for zipcode:', zipcode);
}, process.env.QPS || 100);
