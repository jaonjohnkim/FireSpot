const Promise = require('bluebird');
const initOptions = {
  promiseLib: Promise,
  // connect: (client, dc, isFresh) => {
  //     const cp = client.connectionParameters;
  //     console.log('Connected to database:', cp);
  //   }
}
const uuidv4 = require('uuid/v4');


const pgp = require('pg-promise')(initOptions);
const PQ = require('pg-promise').ParameterizedQuery;

var connection = process.env.HEROKU_POSTGRESQL_PURPLE_URL || 'postgres://postgres:plantlife@localhost:5432/fireincidents';
const client = pgp(connection);

const stringifyDate = (date) => {
  let month = date.getMonth() + 1;
  let dateNum = date.getDate();
  month = month.toString().length === 1 ? "0" + month : month;
  dateNum = dateNum.toString().length === 1 ? "0" + dateNum : dateNum;
  return `${date.getFullYear()}-${month}-${dateNum}T00:00:00.000`;
}

const _getFireIncidentsByDateFromDb = (date) => {
  var strDate = stringifyDate(date);
  var query = new PQ(
    `SELECT * FROM fireincidents WHERE incident_date='${strDate}'`
  );
  return client.query(query);
}

const checkDBForMissingData = async () => {
  console.log('Checking For Missing Data');
  var stack = [];
  var date = new Date();
  date.setDate(date.getDate() - 1);
  //query: where incident_date = yesterday, if yesterday data is 0, then add to stack, and go further back
  console.log('About to get fire incidents by date from db for date:', date);
  var data = await _getFireIncidentsByDateFromDb(date);
  // console.log('data', data);
  while (data.length === 0 && date.getTime() > (new Date('01/01/2003')).getTime()) {
    stack.unshift(stringifyDate(date));
    date.setDate(date.getDate() - 1);
    data = await _getFireIncidentsByDateFromDb(date);
  }
  return stack;
}

const _getZipcodeId = (zipcode) => {
  // console.log('zipcode:', zipcode);
  var query = new PQ(
    `SELECT uuid FROM zipcodes WHERE zipcode = ${zipcode}`
  );
  // console.log('Query:', query);
  return client.any(query)
  // .then(data => {
  //   console.log('GetZipcodeId:', data);
  //   return data;
  // })
  .catch(err => {
    console.error('Error retrieving zipcode:', err, '\nQuery was:', query);
  })
}

const _insertZipcode = (uuid, zipcode, district) => {
  var query = new PQ(
    `INSERT INTO zipcodes(uuid, zipcode, district) VALUES('${uuid}', ${zipcode}, '${district || null}')`
  );
  return client.none(query)
  // .then(data => {
  //   // console.log('Inserting zipcode into DB successful: ', data);
  //   return data[0].id
  // })
  .catch(err => {
    console.error('Error trying to insert new zipcode:', err, '\nQuery was: ', query);
  })
}

const _getOrInsertZipcodeId = (zipcode, district) => {
  // console.log('getOrInsertZipcodeId, zipcode:', zipcode, ' district:', district);
  return _getZipcodeId(zipcode)
  .then(data => {
    if (data.length === 0) {
      // console.log('Requested zipcode ', zipcode,' not found in db, adding to db');
      const uuid = uuidv4();
      return _insertZipcode(uuid, zipcode, district)
      .then(() => {
        return uuid;
      })
    } else {
      // console.log('Requested zipcode ', zipcode,' found in db, id: ', data[0]);
      return data[0].uuid;
    }
  })
}

const insertIntoDB = async (data) => {
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
    let id;
    let result;
    try {
      zipUuid = await _getOrInsertZipcodeId(zipcode, data[zipcode].district);
    } catch (e) {
      console.error('Error getting/inserting zipcode id', e);
    }
    const query = new PQ(
      `INSERT INTO fireincidents(uuid, zipcode, incident_date, incident_count)
      VALUES('${uuidv4()}', '${zipUuid}', '${data[zipcode].date}', ${data[zipcode].incidents})`
    );
    try {
      result = await client.query(query)
    } catch (err) {
      console.error('Error adding new data into the db', err, '\nQuery was:', query);
    }
  }
  // return Promise.all(zipcodes.map(zipcode => {
  //   console.log('Checking db for zipcode:', zipcode, ', district:', data[zipcode].district);
  //   return _getOrInsertZipcodeId(zipcode, data[zipcode].district)
  //   .then(id => {
  //     console.log('Obtained zipcode ID: ', id);
  //     var query = new PQ(
  //       `INSERT INTO fireincidents(zipcode, incident_date, incident_count)
  //       VALUES(${id}, '${data[zipcode].date}', ${data[zipcode].incidents}) RETURNING id`
  //     );
  //     // query.values = [id, data[zipcode].date, data[zipcode].incidents];
  //     return client.any(query)
  //     .then(id => {
  //       console.log('New fire incident entry added:', id);
  //     })
  //     .catch(err => {
  //       console.error('Error adding new data into the db', err, '\nQuery was:', query);
  //     })
  //   })
  // }))
  // .catch(err => {
  //   console.error('Error at Promise.all for inserting into the db', err);
  // })
}

const getFireIncidentsByParamsFromDb = (zipcode, startDate, endDate, granularity) => {
  return _getZipcodeId(zipcode)
  .then(data => {
      // const id = data[0].id;
    // console.log('GetZipcodeId:', data);
    if (data && data.length > 0) {
      const uuid = data[0].uuid;
      var query = new PQ(
        `SELECT date_trunc('${granularity}', incident_date) AS "${granularity}", sum(incident_count)
        FROM fireincidents WHERE incident_date > '${startDate}' AND incident_date < '${endDate}' AND zipcode = '${uuid}'
        GROUP BY ${granularity} ORDER BY ${granularity};`
      );
      return client.any(query)
      .catch(err => {
        console.error('Error getting data from db:', err, '\nQuery was:', query);
      });
    } else {
      return data;
    }
  })
  .catch(err => {
    console.error('Error in getting query from db', err);
  })
}

// Query to grab ALL the zipcodes and add it all up
// SELECT zipcodes.zipcode, zipcodes.district, foo.* FROM (SELECT zipcode, date_trunc('century', incident_date) AS "week", sum(incident_count)
// FROM fireincidents WHERE incident_date > '2003-01-01T00:00:00.000' AND incident_date < '2017-10-23T00:00:00.000'
// GROUP BY zipcode, week ORDER BY week) AS foo INNER JOIN zipcodes ON foo.zipcode = zipcodes.id;

module.exports = {
  stringifyDate,
  checkDBForMissingData,
  insertIntoDB,
  getFireIncidentsByParamsFromDb,
  _getZipcodeId
}
