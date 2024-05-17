'use strict';
const PG = require('../index');

exports.dbOpts = {};

exports.beforeEach = function(tables) {
  beforeEach(async () => {
    exports.dbOpts = {
      host: process.env['DB_HOST'] || 'localhost',
      port: parseInt(process.env['DB_PORT']||'5432'),
      user: process.env['DB_USER'],
      password: process.env['DB_PASSWORD'],
      database: process.env['DB_NAME'],
      ssl: false,
      poolSize: 0,
      //clientIdleTimeout (only for connection pools, default = 30000)
    };
  
    // Drop the test table if one exists
    await exports.dropCreateTables(exports.dbOpts, tables);
  });
}


exports.dropCreateTables = async function(opts, tables) {
  const pg = new PG(opts);
  await pg.initialize({logger:exports.noopLogger});
  let client;
  try {
    client = await pg.getClient();

    // Drop all tables first
    // This needs to be done in reverse order compared to creating them
    const invTables = tables.slice(0);   // reverse() changes the array in-place so we slice(0) first
    invTables.reverse();
    for(const table of invTables) {
      try {
        await pg.exec(client, `DROP TABLE ${table[0]}`);
      }
      catch(err){}
    }

    // Then recreate all tables
    for(const table of tables) {
      const tableName = table[0];
      const tableFields = table.slice(1);
      if (tableFields.length > 0){
        await pg.exec(client, `CREATE TABLE ${tableName}(${tableFields.join(', ')})`);
      }
    }
  }
  finally {
    await pg.releaseClient(client);
    await pg.shutdown();
  }
}


exports.createPg = async function(opts, fnExec) {
  const pg = new PG(opts);
  await pg.initialize({logger:exports.noopLogger});
  let client;
  try {
    client = await pg.getClient();
    await fnExec(pg, client);
  }
  finally {
    await pg.releaseClient(client);
    await pg.shutdown();
  }
}

exports.noopLogger = {
  trace: () => {},
  debug: () => {},
  info:  () => {},
  warn:  () => {},
  error: () => {}
}


