import { Connection, DuckDB } from "../index";

const query = "SELECT count(*) FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')";

describe("Result iterator (streaming)", () => {
  let db: DuckDB;
  let connection: Connection;
  beforeEach(() => {
    db = new DuckDB();
    connection = new Connection(db);
  });

  it("gracefully handles inactive stream", async () => {
    const result1 = await connection.executeIterator(query, false);
    const result2 = await connection.executeIterator(query, false);

    expect(() => result1.fetchRow()).toThrow(
      "No data has been returned (possibly stream has been closed: only one stream can be active on one connection at a time)",
    );
    expect(result2.fetchRow()).toEqual([60]);
  });

  it("gracefully handles inactive stream - second query is materialized", async () => {
    const result1 = await connection.executeIterator(query, false);
    const result2 = await connection.executeIterator(query, true);

    expect(() => result1.fetchRow()).toThrow(
      "No data has been returned (possibly stream has been closed: only one stream can be active on one connection at a time)",
    );
    expect(result2.fetchRow()).toEqual([60]);
  });

  it("works fine if done one after another", async () => {
    const result1 = await connection.executeIterator(query, false);
    expect(result1.fetchRow()).toEqual([60]);
    const result2 = await connection.executeIterator(query, false);
    expect(result2.fetchRow()).toEqual([60]);
  });

  it("is able to close - throws error when reading from closed result", async () => {
    const result1 = await connection.executeIterator("SELECT * FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')");
    expect(result1.fetchRow()).toBeTruthy();
    result1.close();
    expect(result1.isClosed).toBe(true);
    expect(() => result1.fetchRow()).toThrow("Result closed");
  });

  it("works fine if two streaming operations are done on separate connections to one database", async () => {
    const connection1 = new Connection(db);
    const connection2 = new Connection(db);
    const result1 = await connection1.executeIterator(query, false);
    const result2 = await connection2.executeIterator(query, false);
    expect(result1.fetchRow()).toEqual([60]);
    expect(result2.fetchRow()).toEqual([60]);
  });
});
