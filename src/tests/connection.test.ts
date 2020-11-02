import { Connection, DuckDB } from "@addon";

const errorMessage = "Must provide a valid DuckDB object";

describe("Connection class", () => {
  it("accepts a database instance", () => {
    const db = new DuckDB();
    expect(() => new Connection(db)).not.toThrow();
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
    const connection1 = new Connection(db);
    connection1.close();
    expect(connection1.isClosed).toBe(true);
    await expect(() => connection1.executeIterator("SELECT 1")).rejects.toMatchObject({message: "Connection is closed"});
    expect(db.isClosed).toBe(false);
    const connection2 = new Connection(db);
    expect(connection2.isClosed).toBe(false);
    const result = await connection2.executeIterator("SELECT 1");
    expect(result.fetchRow()).toEqual([1]);
  });

  // TODO: We probably want to stop queries when we close connection?
  it("is able to close - in progress queries run to the end", async () => {
    const query1 = "CREATE TABLE test (a INTEGER, b INTEGER);";
    const query2 = "INSERT INTO test SELECT a, b FROM (VALUES (11, 22), (13, 22), (12, 21)) tbl1(a,b), repeat(0, 3000000) tbl2(c)";
    const db = new DuckDB();
    const connection = new Connection(db);
    await connection.executeIterator(query1);
    const p = connection.executeIterator(query2);
    connection.close();
    const resultIterator = await p;
    expect(resultIterator.fetchRow()).not.toBeFalsy();
    expect(connection.isClosed).toBe(true)
  });
});
