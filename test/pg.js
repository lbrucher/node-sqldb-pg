'use strict';
const PG = require('../index');
const should = require('should');

describe('PG driver', () => {
  let dbOpts;
  const tableName = 'test';
  const tableFields = ["id SERIAL PRIMARY KEY", "name TEXT NOT NULL", "zip INTEGER", "city TEXT"];

  beforeEach(async () => {
    dbOpts = {
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
    await dropCreateTables(dbOpts);
  });


  async function dropCreateTables(opts) {
    const pg = new PG(opts);
    await pg.initialize();
    let client;
    try {
      client = await pg.getClient();

      try {
        await pg.exec(client, `DROP TABLE ${tableName}`);
      }
      catch(err){}
  
      await pg.exec(client, `CREATE TABLE ${tableName}(${tableFields.join(', ')})`);
    }
    finally {
      await pg.releaseClient(client);
      await pg.shutdown();
    }
  }


  async function createPg(opts, fnExec) {
    const pg = new PG(opts);
    await pg.initialize({});
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


  it("should initialize", async () => {
    const pg = new PG(dbOpts);
    await pg.initialize({});
  });

  it("should execute a query", async () => {
    await createPg(dbOpts, async (pg, client) => {
      (await pg.exec(client, "INSERT INTO test(name,zip,city) VALUES('john', 1390, 'Nethen')")).should.equal(1);
      (await pg.exec(client, "INSERT INTO test(name,zip,city) VALUES('mary', 1300, 'Jodoigne')")).should.equal(1);
      (await pg.exec(client, "INSERT INTO test(name,zip,city) VALUES('grace', 1390, 'Grez')")).should.equal(1);

      const rows = await pg.query(client, "SELECT * FROM test WHERE zip=1390");
      rows.should.eql([
        {id:1, name:'john', zip:1390, city:'Nethen'},
        {id:3, name:'grace', zip:1390, city:'Grez'}
      ]);
    });
  });

  it("should execute a query returning no data", async () => {
    await createPg(dbOpts, async (pg, client) => {
      (await pg.exec(client, "INSERT INTO test(name,zip,city) VALUES('john', 1390, 'Nethen')")).should.equal(1);
      (await pg.exec(client, "INSERT INTO test(name,zip,city) VALUES('mary', 1300, 'Jodoigne')")).should.equal(1);
 
      const rows = await pg.query(client, "SELECT * FROM test WHERE zip=1200");
      rows.should.eql([]);
    });
  });

  describe("with connection pooling", () => {
    it("should execute a query", async () => {
      const opts = {...dbOpts, poolSize:10};

      await createPg(dbOpts, async (pg) => {
        const clients = [];
        try {
          // create multiple client connections
          clients.push( await pg.getClient() );
          (await pg.exec(clients[0], "INSERT INTO test(name,zip,city) VALUES('john', 1390, 'Nethen')")).should.equal(1);
          (await pg.exec(clients[0], "INSERT INTO test(name,zip,city) VALUES('mary', 1300, 'Jodoigne')")).should.equal(1);

          clients.push( await pg.getClient() );
          (await pg.exec(clients[1], "INSERT INTO test(name,zip,city) VALUES('grace', 1390, 'Grez')")).should.equal(1);

          clients.push( await pg.getClient() );
          (await pg.query(clients[2], "SELECT * FROM test WHERE zip=1300")).should.eql([
            {id:2, name:'mary', zip:1300, city:'Jodoigne'}
          ]);
        }
        finally {
          for(const client of clients){
            await pg.releaseClient(client);
          }
        }
      });
    });
  });

//TODO transactions

});
