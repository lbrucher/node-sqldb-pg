'use strict';
const TH = require('./test-helper');
const PG = require('../index');
const should = require('should');

describe('Transactions', () => {
  const tables = [
    ['address',  "id SERIAL PRIMARY KEY", "street TEXT NOT NULL", "postcode INTEGER NOT NULL", "city TEXT NOT NULL"],
    ["\"user\"", "id SERIAL PRIMARY KEY", "name TEXT NOT NULL UNIQUE", "address_id INTEGER REFERENCES address(id) ON DELETE CASCADE"]
  ];

  TH.beforeEach(tables);



  it("should commit a transaction", async () => {
    await TH.createPg(TH.dbOpts, async (pg, client) => {
      await pg.startTransaction(client, pg.txIsolationLevels.RR);
      (await pg.exec(client, "INSERT INTO address(street,postcode,city) VALUES('Red avenue', 1390, 'Nethen')")).should.equal(1);
      (await pg.exec(client, "INSERT INTO address(street,postcode,city) VALUES('Purple avenue', 1300, 'Jodoigne'),('Green road', 1390, 'Grez')")).should.equal(2);
      (await pg.exec(client, "INSERT INTO \"user\"(name,address_id) VALUES('John', 2),('Mary', 3)")).should.equal(2);

      let rows = await pg.query(client, "SELECT * FROM address WHERE postcode=1390");
      rows.should.eql([
        {id:1, street:'Red avenue', postcode:1390, city:'Nethen'},
        {id:3, street:'Green road', postcode:1390, city:'Grez'}
      ]);

      rows = await pg.query(client, "SELECT * FROM \"user\" ORDER BY name");
      rows.should.eql([
        {id:1, name:'John', address_id:2},
        {id:2, name:'Mary', address_id:3}
      ]);

      await pg.exec(client, "COMMIT");
    });

    // Now check that we can still find those records in the DB
    await TH.createPg(TH.dbOpts, async (pg, client) => {
      const addresses = await pg.query(client, "SELECT * FROM address");
      const users = await pg.query(client, "SELECT name FROM \"user\" ORDER BY name");

      addresses.length.should.equal(3);
      users.should.eql([{name:'John'},{name:'Mary'}]);
    });
  });


  it("should fail a transaction", async () => {
    // Create a user and its adress
    await TH.createPg(TH.dbOpts, async (pg, client) => {
      await pg.startTransaction(client, pg.txIsolationLevels.RR);
      (await pg.exec(client, "INSERT INTO address(street,postcode,city) VALUES('Red avenue', 1390, 'Nethen')")).should.equal(1);
      (await pg.exec(client, "INSERT INTO \"user\"(name,address_id) VALUES('John', 1)")).should.equal(1);
      await pg.exec(client, "COMMIT");
    });

    // Create a second user with the same name as the first user
    try {
      await TH.createPg(TH.dbOpts, async (pg, client) => {
        await pg.startTransaction(client, pg.txIsolationLevels.RR);
        (await pg.exec(client, "INSERT INTO address(street,postcode,city) VALUES('Green avenue', 1300, 'Jodoigne')")).should.equal(1);
        await pg.exec(client, "INSERT INTO \"user\"(name,address_id) VALUES('John', 2)");
        await pg.exec(client, "COMMIT");
        should.fail("Should not get here!");
      });
    }
    catch(err){
      err.code.should.equal('23505');
    }

    // Now verify that the second address and user were in effect not added to the DB
    await TH.createPg(TH.dbOpts, async (pg, client) => {
      const addresses = await pg.query(client, "SELECT * FROM address");
      const users = await pg.query(client, "SELECT * FROM \"user\" ORDER BY name");

      addresses.should.eql([{id:1, street:'Red avenue', postcode:1390, city:'Nethen'}]);
      users.should.eql([{id:1, name:'John', address_id:1}]);
    });
  });


  it("should rollback a transaction", async () => {
    // Create a user and its adress and then rollback instead of commit the transaction
    await TH.createPg(TH.dbOpts, async (pg, client) => {
      await pg.startTransaction(client, pg.txIsolationLevels.RR);
      (await pg.exec(client, "INSERT INTO address(street,postcode,city) VALUES('Red avenue', 1390, 'Nethen')")).should.equal(1);
      (await pg.exec(client, "INSERT INTO \"user\"(name,address_id) VALUES('John', 1)")).should.equal(1);
      await pg.exec(client, "ROLLBACK");
    });

    // Now verify that our DB is still empty
    await TH.createPg(TH.dbOpts, async (pg, client) => {
      const addresses = await pg.query(client, "SELECT * FROM address");
      const users = await pg.query(client, "SELECT * FROM \"user\" ORDER BY name");

      addresses.should.eql([]);
      users.should.eql([]);
    });
  });
});
