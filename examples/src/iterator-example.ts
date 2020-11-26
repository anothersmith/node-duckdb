import { Connection, DuckDB } from "@deepcrawl/node-duckdb";

async function queryDatabaseWithIterator() {
  // create new database in memory
  const db = new DuckDB();
  // create a new connection to the database
  // note you can execute only one streaming query at a time per one connection
  const connection = new Connection(db);

  // by default a query is a streaming one, as opposed to being materialized
  await connection.executeIterator("CREATE TABLE people(id INTEGER, name VARCHAR);");
  await connection.executeIterator("INSERT INTO people VALUES (1, 'Mark'), (2, 'Hannes'), (3, 'Bob');");

  // result is a pointer to the first row
  const result = await connection.executeIterator("SELECT * FROM people;");

  // fetch and print the first row
  console.log(result.fetchRow());

  // fetch and print the rest of rows
  console.log(result.fetchAllRows());

  // release resources
  connection.close();
  db.close();
}

queryDatabaseWithIterator();
