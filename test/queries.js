'use strict';
const TH = require('./test-helper');
const PG = require('../index');
const should = require('should');

describe('Queries', () => {
  const tables = [["test", "id SERIAL PRIMARY KEY", "name TEXT NOT NULL", "zip INTEGER", "city TEXT"]];

  TH.beforeEach(tables);


  it("should initialize", async () => {
    const pg = new PG(TH.dbOpts);
    await pg.initialize({logger:TH.noopLogger});
  });

  it("should execute a query", async () => {
    await TH.createPg(TH.dbOpts, async (pg, client) => {
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
    await TH.createPg(TH.dbOpts, async (pg, client) => {
      (await pg.exec(client, "INSERT INTO test(name,zip,city) VALUES('john', 1390, 'Nethen')")).should.equal(1);
      (await pg.exec(client, "INSERT INTO test(name,zip,city) VALUES('mary', 1300, 'Jodoigne')")).should.equal(1);
 
      const rows = await pg.query(client, "SELECT * FROM test WHERE zip=1200");
      rows.should.eql([]);
    });
  });

  describe("with connection pooling", () => {
    it("should execute a query", async () => {
      const opts = {...TH.dbOpts, poolSize:10};

      await TH.createPg(opts, async (pg) => {
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

});
