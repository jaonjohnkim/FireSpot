var request = require('request');
var Promise = require('bluebird');
var initOptions = {
  promiseLib: Promise,
  connect: (client, dc, isFresh) => {
      const cp = client.connectionParameters;
      console.log('Fire_Incident worker connected to database:', cp.database);
    },
  disconnect: (client, dc) => {
       const cp = client.connectionParameters;
       console.log('Fire_Incident worker disconnecting from database:', cp.database);
    }
}
var pgp = require = require('pg-promise')(initOptions);
// var pg = require('pg');

var connection = process.env.HEROKU_POSTGRESQL_PURPLE_URL || 'postgres://localhost:5432';
var client = pgp(connection);
// client.connect();


var _getFireIncidentsByDateFromDb = (date) => {
  var strDate = `${date.getFullYear()}-${date.getMonth()}-${date.getDate()}T00:00:00.000`;
  return client.query(`SELECT * FROM FireIncidents WHERE incident_date=${strDate}`);
}

var _checkDBForMissingData = async () => {
  var stack = [];
  var date = new Date();
  date.setDate(date.getDate() - 1);
  //query: where incident_date = yesterday, if yesterday data is 0, then add to stack, and go further back
  var data = await _getFireIncidentsByDateFromDb(date);
  while (data.length === 0) {
    stack.push(`${date.getFullYear()}-${date.getMonth()}-${date.getDate()}T00:00:00.000`);
    date.setDate(date.getDate() - 1);
    data = await _getFireIncidentsByDateFromDb(date);
  }
  return stack;
}

var _getZipcodeId = (zipcode, district) => {
  var query = {
    text: `SELECT * FROM Zipcodes WHERE zipcode = ${zipcode} AND district = '${district}'`
  }

  client.query(query, (err, rows, fields) => {

  });
}

var _insertZipcode = (zipcode, district) => {
  var query = {
    text: `INSERT INTO Zipcodes() VALUES()`
  }

  client.query(query, (err, rows, fields) => {

  });
}

var _getOrInsertZipcodeId = (zipcode, district) => {
  _getZipcodeId(zipcode, district)
  .then(data => {
    if (data.length === 0) {
      return _insertZipcode(zipcode, district);
    } else {
      return data;
    }
  })
}

var _processData = (data) => {
  var processed = {
    district: data.neightborhood_district,
    zipcode: data.zipcode,
    incidents: 0
  };

  
}

var _insertIntoDB = (data) => {

  var cols = data[0].getOwnPropertyNames();

  data.forEach(row => {
    var zipcode = row.zipcode;
    var district = row.neighborhood_district;

    _getOrInsertZipcodeId(zipcode, district)
    .then(id => {
      var query = {
        text: `INSERT INTO FireIncidents('zipcode', 'date', 'incidents')})
               VALUES(${zipcode}, ${zipcode}, ${})`
      }

      client.query(query, (err, rows, fields) => {

      })
    })


  })
}

var _getFireIncidentsByDateFromAPI = (date) => {

  var incident_date;
  if (tyepof date === 'object') {
    incident_date = `${date.getFullYear()}-${date.getMonth()}-${date.getDate()}T00:00:00.000`;
  } else {
    incident_date = date;
  }
  console.log('Grabbing Fire Incidents data for date:', incident_date);
  return request.get({
    url: "https://data.sfgov.org/resource/wbb6-uh78.json",
    data: {
      "$$app_token" : process.env.DATASFGOV_KEY,
      "$where" : `incident_date=${incident_date}`
    }
  })
  .catch(err => {
    console.error('Error getting data from DataSF.org:', err);
  });
}

var start = () => {
  var today = new Date();
  _checkDBForMissingData()
  .then(stackOfDates => {
    if (stackOfDates.length) {
      return Promise.all(stackOfDates.map(date => {
        return _getFireIncidentsByDateFromAPI(date)
        .then(_processData)
        .then(_insertIntoDB);
      }))
    } else {
      return _getFireIncidentsByDateFromAPI(today)
      .then(_processData)
      .then(_insertIntoDB);
    }
  })
  .then(result => {
    console.log('Fire Instance Worker finished ')
  })
}

module.exports = {
  start
}
