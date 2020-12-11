import { ConnectionBinding } from "../addon-bindings";
import { ResultStream } from "./result-stream";
import {DuckDB} from "./duckdb";
import { ResultIterator } from "./result-iterator";
import { IExecuteOptions } from "@addon-types";

/**
 * Represents a DuckDB connection.
 * 
 * @remarks
 * A single db instance can have multiple connections. Having more than one connection instance is required when executing concurrent queries.
 * 
 * @public
 */
export class Connection {
  /**
     * Connection constructor.
     * @param duckdb - {@link DuckDB | DuckDB} instance to connect to.
     * 
     * 
     * @example
     * Initializing a connection:
     * ```ts
     * import { DuckDB } from "node-duckdb";
     * const db = new DuckDB();
     * const connection = new Connection(db);
     * ```
     * 
     * @public
     */
  constructor(private duckdb: DuckDB) {}
  private connectionBinding = new ConnectionBinding(this.duckdb.db);
  /**
   * Asynchronously executes the query and returns a node.js stream that wraps the result set.
   * @param command - SQL command to execute
   * @param options - optional options object of type {@link IExecuteOptions | IExecuteOptions}
   * 
   * @example
   * Streaming results of a DuckDB query into a CSV file:
   * ```ts
   * import { Connection, DuckDB, RowResultFormat } from "node-duckdb";
   * import { createWriteStream } from "fs";
   * import { Transform } from "stream";
   * class ArrayToCsvTransform extends Transform {
   *     constructor() {
   *        super({objectMode: true})
   *    }
   *    _transform(chunk: any[], _encoding: string, callback: any) {
   *        this.push(chunk.join(",") + '\n');
   *        callback();
   *    }
   * }
   *
   * async function outputToFileAsCsv() {
   *     const db = new DuckDB();
   *     const connection = new Connection(db);
   *     await connection.execute("CREATE TABLE people(id INTEGER, name VARCHAR);");
   *     await connection.execute("INSERT INTO people VALUES (1, 'Mark'), (2, 'Hannes'), (3, 'Bob');");
   *     const resultStream = await connection.execute("SELECT * FROM people;", {rowResultFormat: RowResultFormat.Array});
   *     const transformToCsvStream = new ArrayToCsvTransform();
   *     const writeStream = createWriteStream("my-people-output");
   *     resultStream.pipe(transformToCsvStream).pipe(writeStream);
   * }
   * outputToFileAsCsv();
   * ```
   */
  public async execute<T>(command: string, options?: IExecuteOptions): Promise<ResultStream<T>> {
    const resultIteratorBinding = await this.connectionBinding.execute<T>(command, options);
    return new ResultStream(new ResultIterator(resultIteratorBinding));
  }
  /**
   * Asynchronously executes the query and returns an iterator that points to the first result in the result set.
   * @param command - SQL command to execute
   * @param options - optional options object of type {@link IExecuteOptions | IExecuteOptions}
   * 
   * @example
   * Printing rows:
   * ```ts
   * import { Connection, DuckDB, RowResultFormat } from "node-duckdb";
   * async function queryDatabaseWithIterator() {
   *   const db = new DuckDB();
   *   const connection = new Connection(db);
   *   await connection.executeIterator("CREATE TABLE people(id INTEGER, name VARCHAR);");
   *   await connection.executeIterator("INSERT INTO people VALUES (1, 'Mark'), (2, 'Hannes'), (3, 'Bob');");
   *   const result = await connection.executeIterator("SELECT * FROM people;");
   *   // print the first row    
   *   console.log(result.fetchRow());
   *   // print the rest of the rows
   *   console.log(result.fetchAllRows());
   *   const result2 = await connection.executeIterator("SELECT * FROM people;", {rowResultFormat: RowResultFormat.Array});
   *   console.log(result2.fetchAllRows());
   *   connection.close();
   *   db.close();
   * }
   * queryDatabaseWithIterator();
   * ```
   * 
   * @example
   * Providing generics type:
   * ```ts
   * const result = await connection.executeIterator<number[]>(`SELECT CAST(1 AS TINYINT)`, {
   *  rowResultFormat: RowResultFormat.Array,
   * });
   * expect(result.fetchRow()).toMatchObject([1]);
   * ```
   */
  public async executeIterator<T>(command: string, options?: IExecuteOptions): Promise<ResultIterator<T>> {
    return new ResultIterator(await this.connectionBinding.execute<T>(command, options));
  }
  /**
   * Close the connection (also closes all {@link ResultStream | ResultStream} or {@link ResultIterator | ResultIterator} objects associated with this connection).
   * @remarks
   * Even though GC will automatically destroy the Connection object at some point, DuckDB data is stored in the native address space, not the V8 heap, meaning you can easily have a Node.js process taking gigabytes of memory (more than the default heap size for Node.js) with V8 not triggering GC. So, definitely think about manually calling `close()`.
   */
  public close(): void {
    return this.connectionBinding.close();
  }
  /**
   * If the connection is closed returns true, otherwise false.
   */
  public get isClosed(): boolean {
    return this.connectionBinding.isClosed;
  }
}
