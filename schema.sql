DROP DATABASE fireincidents IF EXISTS;
CREATE DATABASE fireincidents;
USE fireincidents;

DROP TABLE fireincidents;
DROP TABLE zipcodes;
CREATE TABLE zipcodes (
  index SERIAL,
  uuid VARCHAR(36) PRIMARY KEY,
  zipcode INT,
  district TEXT
);

CREATE TABLE fireincidents (
  index SERIAL,
  uuid VARCHAR(36) PRIMARY KEY,
  zipcode VARCHAR(36),
  incident_date TIMESTAMP,
  incident_count INT,
  FOREIGN KEY (zipcode) REFERENCES zipcodes(uuid)
);
