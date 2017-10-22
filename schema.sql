DROP DATABASE fireincidents IF EXISTS;
CREATE DATABASE fireincidents;
USE fireincidents;

DROP TABLE fireincidents;
DROP TABLE zipcodes;
CREATE TABLE zipcodes (
  id SERIAL PRIMARY KEY,
  zipcode INT,
  district TEXT
);

CREATE TABLE fireincidents (
  id SERIAL PRIMARY KEY,
  zipcode INT,
  incident_date TIMESTAMP,
  incident_count INT,
  FOREIGN KEY (zipcode) REFERENCES zipcodes(id)
);
