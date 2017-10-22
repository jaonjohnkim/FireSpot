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

// var connection = process.env.HEROKU_POSTGRESQL_PURPLE_URL || 'postgres://localhost:5432';
const connection = 'postgres://postgres:plantlife@localhost:5432/fireincidents';
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
  return client.query(query);
}

const _checkDBForMissingData = async () => {
  console.log('Checking For Missing Data');
  var stack = [];
  var date = new Date();
  date.setDate(date.getDate() - 1);
  //query: where incident_date = yesterday, if yesterday data is 0, then add to stack, and go further back
  console.log('About to get fire incidents by date from db for date:', date);
  var data = await _getFireIncidentsByDateFromDb(date);
  // console.log('data', data);
  while (data.length === 0 && date.getTime() > (new Date('10/20/2017')).getTime()) {
    stack.unshift(_stringifyDate(date));
    date.setDate(date.getDate() - 1);
    data = await _getFireIncidentsByDateFromDb(date);
  }
  return stack;
}

const _getZipcodeId = (zipcode) => {
  var query = new PQ(
    `SELECT * FROM zipcodes WHERE zipcode = ${zipcode}`
  );
  return client.oneOrNone(query)
  .catch(err => {
    console.error('Error trying to insert new zipcode:', err);
  })
}

const _insertZipcode = (zipcode, district) => {
  var query = new PQ(
    `INSERT INTO zipcodes(zipcode, district) VALUES(${zipcode}, ${district})`
  );
  return client.one(query)
  .then(data => {
    console.log('Inserting zipcode into DB successful: ', data.id);
    return data.id
  })
  .catch(err => {
    console.error('Error trying to insert new zipcode:', err);
  })
}

const _getOrInsertZipcodeId = (zipcode, district) => {
  return _getZipcodeId(zipcode)
  .then(data => {
    console.log('GET ZIPCODE ID RESPONSE:', data);
    if (!data) {
      return _insertZipcode(zipcode, district);
    } else {
      console.log('Found zipcode ID:', data);
      return data[0];
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
  // console.log('Pre-processed Data:', data[0]);
  return data.reduce((acc, row) => {
    // console.log('Accumulator:', acc);
    if (acc[row.zipcode] !== undefined) {
      var dataByZipcode = acc[row.zipcode];
      dataByZipcode.incidents += 1;
    } else if (row.zipcode){
      acc[row.zipcode] = {
        district: row.neightborhood_district,
        incidents: 1,
        date: row.incident_date
      };
    }
    return acc;
  }, {});
}

const _insertIntoDB = (data) => {
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
  console.log('Zipcodes:', zipcodes);
  Promise.all(zipcodes.map(zipcode => {
    return _getOrInsertZipcodeId(zipcode, data[zipcode].district)
    .then(id => {
      console.log('Obtained zipcode ID: ', id);
      var query = new PQ(
        `INSERT INTO fireincidents('zipcode', 'incident_date', 'incident_count')}) VALUES($1, $2, $3)`
      );
      query.values = [id, data[zipcode].date, data[zipcode].incidents];
      return client.none(query)
    })
  }))
  .then(() => {
    console.log('Finished adding all the new data into the db');
  })
  .catch(err => {
    console.error('Error adding new data into the db', err);
  })
}

const _getFireIncidentsByDateFromAPI = (date) => {
  let incident_date;
  if (typeof date === 'object') {
    incident_date = _stringifyDate(date);
  } else {
    incident_date = date;
  }
  console.log('Grabbing Fire Incidents data for date:', incident_date);
  return request({
    method: 'GET',
    url: "https://data.sfgov.org/resource/wbb6-uh78.json",
    qs: {
      $$app_token : process.env.DATASFGOV_KEY || 'xdD9TSiPqAKYnSOab3U0AexMU',
      $where : `incident_date='${'2003-01-01T00:00:00.000' || incident_date}'`,
      $limit : 10
    }
  })
  .catch(err => {
    console.error('Error getting data from DataSF.org:', err);
  });
}

const start = () => {
  var today = new Date();
  console.log("Worker starting for date:", today.toString());
  _checkDBForMissingData()
  .then(stackOfDates => {
    if (stackOfDates.length) {
      return Promise.all(stackOfDates.map(date => {
        return _getFireIncidentsByDateFromAPI(date)
        .then(data => {
          // console.log('Response Data:', data);
          var processed = _processData(JSON.parse(data));
          return _insertIntoDB(processed);
        })
      }))
    } else {
      return _getFireIncidentsByDateFromAPI(today)
      .then(data => {
        var processed = _processData(data);
        return _insertIntoDB(processed);
      })
    }
  })
  .then(result => {
    console.log('Fire Instance Worker finished ')
  })
  .catch(err => {
    console.error('Error with Worker:', err);
  })
}

module.exports = {
  start
}
