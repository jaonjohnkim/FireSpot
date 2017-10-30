const request = require('request-promise');

setInterval(() => {
  // const zipcodes = [94102, 94111];
  // let zipcode = zipcodes[Math.round(Math.random())];
  zipcode = 94111;
  request(`https://fireincident.herokuapp.com/json?zipcode=${zipcode}&startDate=2017-07-01T00:00:00.000&endDate=2017-10-25T00:00:00.000&granularity=month`)
  console.log('Pinged for zipcode:', zipcode);
}, 50);
