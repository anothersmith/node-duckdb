import { Connection, DuckDB } from "@addon";
import { IExecuteOptions, RowResultFormat } from "@addon-types";

const errorMessage = "Must provide a valid DuckDB object";

const executeOptions: IExecuteOptions = { rowResultFormat: RowResultFormat.Array };

describe("Connection class", () => {
  it("accepts a database instance", async () => {
    const db = new DuckDB();
    await db.init();
    expect(() => new Connection(db)).not.toThrow();
    db.close();
  });

  it("throws if not given a database", () => {
    expect(() => new (<any>Connection)()).toThrowError("Cannot read property 'db' of undefined");
  });

  it("throws if given a non-database object", () => {
    expect(() => new (<any>Connection)({ notADatabase: "true" })).toThrowError(errorMessage);
  });

  it("throws if given a primitive", () => {
    expect(() => new (<any>Connection)(1)).toThrowError(errorMessage);
  });

  it("is able to be closed - but does not close the database", async () => {
    const db = new DuckDB();
    await db.init();
    const connection1 = new Connection(db);
    connection1.close();
    expect(connection1.isClosed).toBe(true);
    await expect(() => connection1.executeIterator("SELECT 1")).rejects.toMatchObject({
      message: "Connection is closed",
    });
    expect(db.isClosed).toBe(false);
    const connection2 = new Connection(db);
    expect(connection2.isClosed).toBe(false);
    const result = await connection2.executeIterator("SELECT 1", executeOptions);
    expect(result.fetchRow()).toEqual([1]);
    connection2.close();
    db.close();
  });

  // TODO: We probably want to stop queries when we close connection?
  it("is able to close - in progress queries run to the end", async () => {
    const query1 = "CREATE TABLE test (a INTEGER, b INTEGER);";
    const query2 =
      "INSERT INTO test SELECT a, b FROM (VALUES (11, 22), (13, 22), (12, 21)) tbl1(a,b), repeat(0, 30000) tbl2(c)";
    const db = new DuckDB();
    await db.init();
    const connection = new Connection(db);
    await connection.executeIterator(query1, executeOptions);
    const p = connection.executeIterator(query2, executeOptions);
    connection.close();
    const resultIterator = await p;
    expect(resultIterator.fetchRow()).not.toBeFalsy();
    expect(connection.isClosed).toBe(true);
    connection.close();
    db.close();
  });
});
