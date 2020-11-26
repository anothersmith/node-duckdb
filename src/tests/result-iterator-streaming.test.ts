import { Connection, DuckDB } from "@addon";
import { IExecuteOptions, RowResultFormat } from "@addon-types";

const query = "SELECT count(*) FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')";

const executeOptions: IExecuteOptions = { rowResultFormat: RowResultFormat.Array, forceMaterialized: false };

describe("Result iterator (streaming)", () => {
  let db: DuckDB;
  let connection: Connection;
  beforeEach(() => {
    db = new DuckDB();
    connection = new Connection(db);
  });

  afterEach(() => {
    connection.close();
    db.close();
  });

  it("gracefully handles inactive stream", async () => {
    const result1 = await connection.executeIterator(query, executeOptions);
    const result2 = await connection.executeIterator(query, executeOptions);

    expect(() => result1.fetchRow()).toThrow(
      "No data has been returned (possibly stream has been closed: only one stream can be active on one connection at a time)",
    );
    expect(result2.fetchRow()).toEqual([BigInt(60)]);
  });

  it("gracefully handles inactive stream - second query is materialized", async () => {
    const result1 = await connection.executeIterator(query, executeOptions);
    const result2 = await connection.executeIterator(query, {
      rowResultFormat: RowResultFormat.Array,
      forceMaterialized: true,
    });

    expect(() => result1.fetchRow()).toThrow(
      "No data has been returned (possibly stream has been closed: only one stream can be active on one connection at a time)",
    );
    expect(result2.fetchRow()).toEqual([BigInt(60)]);
  });

  it("works fine if done one after another", async () => {
    const result1 = await connection.executeIterator(query, executeOptions);
    expect(result1.fetchRow()).toEqual([BigInt(60)]);
    const result2 = await connection.executeIterator(query, executeOptions);
    expect(result2.fetchRow()).toEqual([BigInt(60)]);
  });

  it("is able to close - throws error when reading from closed result", async () => {
    const result1 = await connection.executeIterator(
      "SELECT * FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')",
    );
    expect(result1.fetchRow()).toBeTruthy();
    result1.close();
    expect(result1.isClosed).toBe(true);
    expect(() => result1.fetchRow()).toThrow("Result closed");
  });

  it("works fine if two streaming operations are done on separate connections to one database", async () => {
    const connection1 = new Connection(db);
    const connection2 = new Connection(db);
    const result1 = await connection1.executeIterator(query, executeOptions);
    const result2 = await connection2.executeIterator(query, executeOptions);
    expect(result1.fetchRow()).toEqual([BigInt(60)]);
    expect(result2.fetchRow()).toEqual([BigInt(60)]);
  });

  it("works fine if two streaming operations are done on separate databases", async () => {
    const query1 = "CREATE TABLE test (a INTEGER, b INTEGER);";
    const query2 =
      "INSERT INTO test SELECT a, b FROM (VALUES (11, 22), (13, 22), (12, 21)) tbl1(a,b), repeat(0, 3000) tbl2(c)";
    const query3 = "SELECT * FROM test ORDER BY a ASC;";
    const db1 = db;
    const db2 = new DuckDB();
    const connection1 = connection;
    const connection2 = new Connection(db2);
    await Promise.all([
      connection1.executeIterator(query1, executeOptions),
      connection2.executeIterator(query1, executeOptions),
    ]);
    await Promise.all([
      connection1.executeIterator(query2, executeOptions),
      connection2.executeIterator(query2, executeOptions),
    ]);
    const [result1, result2] = await Promise.all([
      connection1.executeIterator(query3, executeOptions),
      connection2.executeIterator(query3, executeOptions),
    ]);
    expect(result1.fetchRow()).toEqual([11, 22]);
    expect(result2.fetchRow()).toEqual([11, 22]);
    connection1.close();
    connection2.close();
    db1.close();
    db2.close();
  });
});
