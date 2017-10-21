var express = require('express');
var db = require('../db/index.js')
var amqp = require('amqplib/callback_api'); // message bus
var request = require('request');

amqp.connect(process.env.CLOUDAMQP_URL)

var app = express();

app.get('/', (req, res) => {

});

app.listen(process.env.PORT || 3000, () => {
  console.log('API server for Fire Instance is LIVE!');
});
