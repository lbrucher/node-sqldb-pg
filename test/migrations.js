'use strict';
const TH = require('./test-helper');
const PG = require('../index');
const should = require('should');

describe('Migrations', () => {

  TH.beforeEach([["migs"]]);

  describe('ensureMigrationsTable', () => {
    it("should create a migration table when it does not exist", async () => {
      await TH.createPg(TH.dbOpts, async (pg, client) => {
        // Ensure there is no 'migs' table
        try{
          await pg.query(client, "SELECT * FROM migs");
          should.fail("Should not get here!");
        }
        catch(err){
          err.code.should.equal('42P01');
        }

        // Create the migs table
        await pg.ensureMigrationsTable('migs');

        // Query should now work
        const rows = await pg.query(client, "SELECT * FROM migs");
        rows.should.eql([]);
      });
    });

    it("should not create a migration table when one already exist", async () => {
      await TH.createPg(TH.dbOpts, async (pg, client) => {
        // Create the migs table
        await pg.exec(client, "CREATE TABLE migs(name varchar(255) NOT NULL PRIMARY KEY, updated_at timestamp NOT NULL)");
        let rows = await pg.query(client, "SELECT * FROM migs");
        rows.should.eql([]);

        // Do not re-create the migs table
        await pg.ensureMigrationsTable('migs');

        // Query should still work
        rows = await pg.query(client, "SELECT * FROM migs");
        rows.should.eql([]);
      });
    });
  });


  describe('listExecutedMigrationNames', () => {
    it("should return an empty list when there are no completed migrations", async () => {
      await TH.createPg(TH.dbOpts, async (pg, client) => {
        await pg.ensureMigrationsTable('migs');
        const names = await pg.listExecutedMigrationNames('migs');
        names.should.eql([]);
      });
    });

    it("should return the list of completed migrations", async () => {
      await TH.createPg(TH.dbOpts, async (pg, client) => {
        await pg.ensureMigrationsTable('migs');

        const now = Date.now();
        await pg.exec(client, "INSERT INTO migs(name, updated_at) VALUES('001-init',$1),('002-blah',$2)", [pg.dateIso(now), pg.dateIso(now+10000)]);

        const names = await pg.listExecutedMigrationNames('migs');
        names.should.eql(['001-init', '002-blah']);
      });
    });
  });


  describe('logMigrationSuccessful', () => {
    it("should log migrations", async () => {
      await TH.createPg(TH.dbOpts, async (pg, client) => {
        await pg.ensureMigrationsTable('migs');
        (await pg.query(client, "SELECT name FROM migs ORDER BY name")).should.eql([]);

        const conn = {
          exec: (sql, params) => pg.exec(client, sql, params)
        };
        await pg.logMigrationSuccessful(conn, 'migs', '1-mig');
        await pg.logMigrationSuccessful(conn, 'migs', '2-mig');
        (await pg.query(client, "SELECT name FROM migs ORDER BY name")).should.eql([{name:'1-mig'},{name:'2-mig'}]);
      });
    });
  });


  it("should expose the transaction isolation level to be used during migrations", async () => {
    await TH.createPg(TH.dbOpts, async (pg, client) => {
      pg.getMigrationTransactionIsolationLevel().should.equal('rr');
    });
  });

});
