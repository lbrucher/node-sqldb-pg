'use strict';
const { driverPrototype } = require('node-sqldb');
const { Client, Pool } = require('pg');


const transaction_isolation_levels = {
	'rc':  'READ COMMITTED',
	'rr':  'REPEATABLE READ',
	'ser': 'SERIALIZABLE'
};

const defaultOptions = {
  poolSize: 10,
  clientIdleTimeout: 30000
};


// Options = {
//    host: ''
//    port: 0
//    user: ''
//    password: ''
//    database: ''
//    ssl: true|false
//    sslRejectUnauthorized: true|false
//    poolSize: 0 (0 = no pooling, default = 10)
//    clientIdleTimeout (only for connection pools, default = 30000)
// }
function PG(options) {
  let logger;
  let dbPool;
  let numActiveClients = 0;


  function getConnectionOpts() {
    const opts = {
      host:     options.host,
      port:     options.port,
      user:     options.username,
      password: options.password,
      database: options.database,
    };

    if (options.ssl){
      opts.ssl = { rejectUnauthorized: options.sslRejectUnauthorized === true };
    }

    return opts;
  }


  async function createPool(){
    const opts = getConnectionOpts();
    dbPool = new Pool({
      ...opts, 
      max: options.poolSize || defaultOptions.poolSize, // max number of clients in the pool
      idleTimeoutMillis: options.clientIdleTimeout || defaultOptions.clientIdleTimeout, // how long a client is allowed to remain idle before being closed
    });

    dbPool.on('error', (err,client) => {
      logger.error("Postgres Pool error: ", err.message, err.stack);
    });

    logger.info("Created Postgres DB pool");
  }


  async function destroyPool(){
    if (dbPool){
      logger.info("Destorying PG pool. Num active clients: %d", numActiveClients);
      let p = dbPool;
      dbPool = null;

      await p.end();
    }
  }


  async function recreatePool(){
    await destroyPool();
    await createPool();
  }


  async function getPooledClient(){
    let connectAttempts = 0;
    while(true) {
      try {
        connectAttempts++;
        const client = await dbPool.connect();
        numActiveClients++;
        return client;
      }
      catch(err) {
        // if it's the first attempt, try recreating the db pool...
        if (connectAttempts === 1){
          logger.warn('Error fetching client from pool, will recreate the db pool and retry... Err: ', err);
          await recreatePool();
        }
        else{
          logger.error('Error fetching client from pool: ', err);
          throw err;
        }
      }
    }
  }

  async function releasePooledClient(client){
    try{
      numActiveClients--;
      client.release();
    }
    catch(err) {
    }
  }


  this.initialize = async function(opts = {}) {
    logger = opts.logger || this.logger;
    if ((options.poolSize||0) !== 0) {
      await createPool();
    }
  }

  this.shutdown = async function() {
    if (!!dbPool) {
      await destroyPool();
    }
  }

  this.getClient = async function() {
    let client;
    if (!!dbPool) {
      client = await getPooledClient();
    }
    else {
      client = new Client(getConnectionOpts());
      await client.connect();
      numActiveClients++;
    }

    return client;
  }


  this.releaseClient = async function(client) {
    if (!!dbPool) {
      await releasePooledClient(client);
    }
    else {
      try{
        numActiveClients--;
        await client.end();
      }
      catch(e) {}
    }
  }


  /*
   * Return an array of rows, or [] if the query returned no data
   */
  this.query = async function(client, sql, params) {
    const res = await client.query(sql, params);

    // It's important to check rowCount because rows[]'s length might be different
    if (res.rowCount === 0){
      return [];
    }
    else{
      // we might have a rowCount > 0 but rows = [] (INSERT INTO for instance)
      const rows = res.rows || [];
      while(rows.length < res.rowCount)
        rows.push(null);
      return rows;
    }
  }


  /*
   * Return the number of rows affected by the query
   */
  this.exec = async function(client, sql, params) {
    const rows = await this.query(client, sql, params);
    return rows.length;
  }


  this.startTransaction = async function(client, tx_isolation_level) {
    const tx_level = transaction_isolation_levels[tx_isolation_level];
    if (tx_level == null){
      logger.error("Invalid tx isolation level [%s]!", tx_isolation_level);
      throw new Error("Invalid transaction isolation level!");
    }

    await client.query('BEGIN');
    await client.query('SET TRANSACTION ISOLATION LEVEL '+tx_level);
  }

  // Optional method, defaults to: client.query('COMMIT')
  // async function commitTransaction(client) {
  // }

  // Optional method, defaults to: client.query('ROLLBACK')
  // async function rollbackTransaction(client) {
  // }
}


Object.assign(PG.prototype, driverPrototype);
module.exports = PG;
