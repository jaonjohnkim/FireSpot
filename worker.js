const request = require('request-promise');
const Promise = require('bluebird');
const initOptions = {
  promiseLib: Promise
  // connect: (client, dc, isFresh) => {
  //     const cp = client.connectionParameters;
  //     console.log('Fire_Incident worker connected to database:', cp.database);
  //   },
  // disconnect: (client, dc) => {
  //      const cp = client.connectionParameters;
  //      console.log('Fire_Incident worker disconnecting from database:', cp.database);
  //   }
}
const pgp = require('pg-promise')(initOptions);
const PQ = require('pg-promise').ParameterizedQuery;
// var pg = require('pg');

var connection = process.env.HEROKU_POSTGRESQL_PURPLE_URL || 'postgres://postgres:plantlife@localhost:5432/fireincidents';
const client = pgp(connection);
// client.connect();

const _stringifyDate = (date) => {
  let month = date.getMonth() + 1;
  let dateNum = date.getDate();
  month = month.toString().length === 1 ? "0" + month : month;
  dateNum = dateNum.toString().length === 1 ? "0" + dateNum : dateNum;
  return `${date.getFullYear()}-${month}-${dateNum}T00:00:00.000`;
}

const _getFireIncidentsByDateFromDb = (date) => {
  var strDate = _stringifyDate(date);
  var query = new PQ(
    `SELECT * FROM fireincidents WHERE incident_date='${strDate}'`
  );
  return client.query(query)
}

const _checkDBForMissingData = async () => {
  console.log('Checking For Missing Data');
  var stack = [];
  var date = new Date();
  date.setDate(date.getDate() - 1);
  //query: where incident_date = yesterday, if yesterday data is 0, then add to stack, and go further back
  console.log('About to get fire incidents by date from db for date:', date);
  var data = await _getFireIncidentsByDateFromDb(date);
  console.log('data', data);
  while (data.length === 0 && date.getTime() > (new Date('01/01/2003')).getTime()) {
    stack.unshift(_stringifyDate(date));
    date.setDate(date.getDate() - 1);
    data = await _getFireIncidentsByDateFromDb(date);
  }
  return stack;
}

const _getZipcodeId = (zipcode) => {
  var query = new PQ(
    `SELECT id FROM zipcodes WHERE zipcode = ${zipcode}`
  );
  return client.any(query)
  .catch(err => {
    console.error('Error retrieving zipcode:', err, '\nQuery was:', query);
  })
}

const _insertZipcode = (zipcode, district) => {
  var query = new PQ(
    `INSERT INTO zipcodes(zipcode, district) VALUES(${zipcode}, '${district || null}') RETURNING id`
  );
  return client.any(query)
  .then(data => {
    console.log('Inserting zipcode into DB successful: ', data);
    return data[0].id
  })
  .catch(err => {
    console.error('Error trying to insert new zipcode:', err, '\nQuery was: ', query);
  })
}

const _getOrInsertZipcodeId = (zipcode, district) => {
  console.log('getOrInsertZipcodeId, zipcode:', zipcode, ' district:', district);
  return _getZipcodeId(zipcode)
  .then(data => {
    if (data.length === 0) {
      console.log('Requested zipcode ', zipcode,' not found in db, adding to db');
      return _insertZipcode(zipcode, district);
    } else {
      console.log('Requested zipcode ', zipcode,' found in db, id: ', data[0]);
      return data[0].id;
    }
  })
}

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

const _insertIntoDB = async (data) => {
  // Input:
  // data = {
  //   "91405": {
  //     district: 'Tenderloin',
  //     incidents: 10,
  //     date: '2017-10-19T00:00:00.000'
  //   }
  // }
  // console.log('DATA:', data);
  var zipcodes = Object.getOwnPropertyNames(data);
  // console.log('Zipcodes:', zipcodes);
  // console.log('Data:', data);
  for (let i = 0; i < zipcodes.length; i++) {
    const zipcode = zipcodes[i];
    const id = await _getOrInsertZipcodeId(zipcode, data[zipcode].district);
    const query = new PQ(
      `INSERT INTO fireincidents(zipcode, incident_date, incident_count)
      VALUES(${id}, '${data[zipcode].date}', ${data[zipcode].incidents})`
    );
    return client.query(query)
    .catch(err => {
      console.error('Error adding new data into the db', err, '\nQuery was:', query);
    })
  }
  return Promise.all(zipcodes.map(zipcode => {
    console.log('Checking db for zipcode:', zipcode, ', district:', data[zipcode].district);
    return _getOrInsertZipcodeId(zipcode, data[zipcode].district)
    .then(id => {
      console.log('Obtained zipcode ID: ', id);
      var query = new PQ(
        `INSERT INTO fireincidents(zipcode, incident_date, incident_count)
        VALUES(${id}, '${data[zipcode].date}', ${data[zipcode].incidents}) RETURNING id`
      );
      // query.values = [id, data[zipcode].date, data[zipcode].incidents];
      return client.any(query)
      .then(id => {
        console.log('New fire incident entry added:', id);
      })
      .catch(err => {
        console.error('Error adding new data into the db', err, '\nQuery was:', query);
      })
    })
  }))
  .catch(err => {
    console.error('Error at Promise.all for inserting into the db', err);
  })
}

const _getFireIncidentsByDateFromAPI = (date) => {
  let incident_date = typeof date === 'object' ? _stringifyDate(date) : date;
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
  var today = new Date();
  console.log("Worker starting for date:", today.toString());
  _checkDBForMissingData()
  .then(async (stackOfDates) => {
    if (stackOfDates.length) {
      for (var i = 0; i < stackOfDates.length; i++) {
        let data = await _getFireIncidentsByDateFromAPI(stackOfDates[i]);
        let processed = _processData(JSON.parse(data));
        let result = await _insertIntoDB(processed);
      }
      // console.log('getting data from API for MISSING DATES', stackOfDates);
      // return Promise.all(stackOfDates.map(date => {
      //   return _getFireIncidentsByDateFromAPI(date)
      //   .then(data => {
      //     // console.log('Response Data:', data);
      //     var processed = _processData(JSON.parse(data));
      //     return _insertIntoDB(processed);
      //   })
      // }))
    } else {
      console.log('getting data from API for TODAY');
      return _getFireIncidentsByDateFromAPI(today)
      .then(data => {
        var processed = _processData(data);
        return _insertIntoDB(processed);
      })
    }
  })
  .then(result => {
    console.log('Fire Instance Worker finished ');
  })
  .catch(err => {
    console.error('Error with Worker:', err);
  })
}

module.exports = {
  start
}
