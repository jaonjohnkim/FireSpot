const redis = require('redis');
const Promise = require('bluebird');
Promise.promisifyAll(redis.RedisClient.prototype);
Promise.promisifyAll(redis.Multi.prototype);
const client = redis.createClient(process.env.REDISTOGO_URL);

client.on('error', err => {
  console.log('Error ' + err);
})

const getFromCache = (query) => {
  return client.getAsync(JSON.stringify(query));
}

const addToCache = (query, result, expiration) => {
  //custom query will cache for 10 sec
  expiration = expiration || 10;
  // console.log('Adding cache with key:', JSON.stringify(query));
  // console.log('And value:', JSON.stringify(result));
  client.setAsync(JSON.stringify(query), JSON.stringify(result), 'EX', expiration)
  .then(data => {
    console.log('CACHE Return:', data);
    console.log('Successfully cached');
  })
  .error(err => {
    console.error('Error in caching', err);
  });
}

module.exports = {
  addToCache,
  getFromCache
}
