const request = require('request-promise');
const Promise = require('bluebird');
const db = require('./db/index.js');
const redis = require('./server/redisHelper.js');
const statsD = require('node-statsd');
const statsDClient = new statsD({
  host: 'statsd.hostedgraphite.com',
  port: 8125,
  prefix: process.env.HOSTEDGRAPHITE_APIKEY
});

const _processData = (data) => {
  // Input: Array of objects
  // Output:
  // returnData = {
  //   "91405": {
  //     district: 'Tenderloin',
  //     incidents: 10,
  //     date: '2017-10-19T00:00:00.000'
  //   },
  //   "91141" : {
  //     district: 'Example',
  //     incidents: 4,
  //     date: '2017-10-19T00:00:00.000'
  //   }
  // }
  return data.reduce((acc, row) => {
    if (acc[row.zipcode] !== undefined) {
      var dataByZipcode = acc[row.zipcode];
      dataByZipcode.incidents += 1;
    } else if (row.zipcode){
      acc[row.zipcode] = {
        district: row.neighborhood_district,
        incidents: 1,
        date: row.incident_date
      };
    }
    return acc;
  }, {});
}

const _getFireIncidentsByDateFromAPI = (date) => {
  let incident_date = typeof date === 'object' ? db.stringifyDate(date) : date;
  console.log('Grabbing Fire Incidents data for date:', incident_date);
  return request({
    method: 'GET',
    url: "https://data.sfgov.org/resource/wbb6-uh78.json",
    qs: {
      $$app_token : process.env.DATASFGOV_KEY,
      $where : `incident_date='${incident_date || '2003-01-01T00:00:00.000'}'`,
      $limit : 10000
    }
  })
  .catch(err => {
    console.error('Error getting data from DataSF.org:', err);
  });
}

// const processAndInsertIntoDb = async (stackOfDates) => {
//   for (var i = 0; i < stackOfDates.length; i++) {
//     let data = await _getFireIncidentsByDateFromAPI(stackOfDates[i]);
//     let processed = _processData(JSON.parse(data));
//     let result = await _insertIntoDB(processed);
//     return result;
//   }
// }

const start = () => {
  const today = new Date();
  const start = Date.now();
  console.log("Worker starting for date:", today.toString());
  db.checkDBForMissingData()
  .then(async (stackOfDates) => {
    // if (stackOfDates.length) {
      for (var i = 0; i < stackOfDates.length; i++) {
        let data;
        let result;
        try {
          data = await _getFireIncidentsByDateFromAPI(stackOfDates[i]);
        } catch (e) {
          console.error('Error getting fire incidents data from API:', e);
        }
        const processed = _processData(JSON.parse(data));
        try {
          result = await db.insertIntoDB(processed);
        } catch (e) {
          console.error('Error inserting new data into local db', e);
        }
      }
      // console.log('getting data from API for MISSING DATES', stackOfDates);
      // return Promise.all(stackOfDates.map(date => {
      //   return _getFireIncidentsByDateFromAPI(date)
      //   .then(data => {
      //     // console.log('Response Data:', data);
      //     var processed = _processData(JSON.parsestatsd.hostedgraphite.com(data));
      //     return _insertIntoDB(processed);
      //   })
      // }))
    // } else {
    //   console.log('getting data from API for TODAY');
    //   return _getFireIncidentsByDateFromAPI(today)
    //   .then(data => {
    //     var processed = _processData(data);
    //     return db.insertIntoDB(processed);
    //   })
    // }
  })
  .then(() => {
    console.log('Fire Instance Worker finished updating, now adding pre-fetched cache');
    const defaultCachedZipcodes = [94102, 94103, 94109, 94129, 94130];
    defaultCachedZipcodes.forEach(zipcode => {
      const today = db.stringifyDate(new Date());
      let threeMonthsAgo = new Date();
      threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);
      const threeMonthsAgoStr = db.stringifyDate(threeMonthsAgo);
      db.getFireIncidentsByParamsFromDb(zipcode, threeMonthsAgoStr, today, 'month')
      .then(data => {
        redis.addToCache({
          zipcode: zipcode.toString(),
          startDate: threeMonthsAgoStr,
          endDate: today,
          granularity: 'month'
        }, data, 23*60*60+59*60); //save default cache for 23 hrs and 59 min
      });
    })
  })
  .then(() => {
    const totalActive = Date.now() - start;
    statsDClient.histogram('worker.time.active', totalActive);
  })
  .catch(err => {
    console.error('Error with Worker:', err);
    statsDClient.increment('worker.fail');
  })
}

module.exports = {
  start
}
