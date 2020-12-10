export { DuckDB, Connection, ResultIterator, ResultStream } from "./addon";
export * from "./addon-types";

/**
 * Node-DuckDB is a thin wrapper on top of {@link https://duckdb.org/ | DuckDB}. 
 * 
 * Using it involves:
 * 
 * 1. Creating a {@link DuckDB | DuckDB} object
 * 
 * 2. Creating a {@link Connection | Connection} object to the DuckDB object
 * 
 * 3. Calling {@link Connection.execute | Connection.execute} or {@link Connection.executeIterator | Connection.executeIterator} on the Connection object
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
 * For more examples see {@link https://github.com/deepcrawl/node-duckdb/tree/feature/ODIN-423-welcome-page/examples | here}.
 */
