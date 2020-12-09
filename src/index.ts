/**
 * Node.js bindings for DuckDB from {@link https://www.deepcrawl.com/ | DeepCrawl}.
 * 
 * @packageDocumentation 
 * 
 * @example
 * Do some simple querying and print the result
 * ```
 * import { Connection, DuckDB } from "node-duckdb";
 * 
 * async function queryDatabaseWithIterator() {
 *   // create new database in memory
 *   const db = new DuckDB();
 *   // create a new connection to the database
 *  const connection = new Connection(db);
 *
 *  // perform some queries
 *  await connection.executeIterator("CREATE TABLE people(id INTEGER, name VARCHAR);");
 *  await connection.executeIterator("INSERT INTO people VALUES (1, 'Mark'), (2, 'Hannes'), (3, 'Bob');");
 *  const result = await connection.executeIterator("SELECT * FROM people;");
 *
 *  // fetch and print result
 *  console.log(result.fetchAllRows());

 *  // release resources
 *  connection.close();
 *  db.close();
 * }
 *
 * queryDatabaseWithIterator();
 * ```
 */

export { DuckDB, Connection, ResultIterator, ResultStream } from "./addon";
export * from "./addon-types";
