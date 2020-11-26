import { Connection, DuckDB } from "@addon";
import { IExecuteOptions, RowResultFormat } from "@addon-types";

import { readStream } from "./utils";

const query = "SELECT * FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')";

const executeOptions: IExecuteOptions = { rowResultFormat: RowResultFormat.Array };


describe("Result stream", () => {
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

  it("reads a csv", async () => {
    const rs = await connection.execute<any>(query, executeOptions);
    const elements = await readStream(rs);
    expect(elements.length).toBe(60);
    expect(elements[0]).toEqual([
      1,
      "AAAAAAAABAAAAAAA",
      873244800000,
      null,
      2450810,
      2452620,
      "Y",
      98539,
      "http://www.foo.com",
      "welcome",
      2531,
      8,
      3,
      4,
    ]);
  });

  it("is able to read from two streams sequentially", async () => {
    const rs1 = await connection.execute<any>(query, executeOptions);
    const elements1 = await readStream(rs1);
    expect(elements1.length).toBe(60);

    const rs2 = await connection.execute<any>(query, executeOptions);
    const elements2 = await readStream(rs2);
    expect(elements2.length).toBe(60);
  });

  it("correctly handles errors - closes resource", async () => {
    const rs1 = await connection.execute<any>(query, executeOptions);
    await connection.execute(query, executeOptions);
    let hasClosedFired = false;
    rs1.on("close", () => (hasClosedFired = true));
    await expect(readStream(rs1)).rejects.toMatchObject({
      message:
        "No data has been returned (possibly stream has been closed: only one stream can be active on one connection at a time)",
    });
    expect(hasClosedFired).toBe(true);
  });

  it("closes resource when all data has been read", async () => {
    const rs = await connection.execute<any>(query, executeOptions);
    let hasClosedFired = false;
    rs.on("close", () => (hasClosedFired = true));
    const elements = await readStream(rs);
    expect(elements.length).toBe(60);
    expect(hasClosedFired).toBe(true);
  });

  it("closes resource on manual destroy", async () => {
    const rs1 = await connection.execute<any>(query, executeOptions);
    let hasClosedFired = false;
    rs1.on("close", () => (hasClosedFired = true));
    void readStream(rs1);
    expect(hasClosedFired).toBe(false);
    rs1.destroy();
    expect(hasClosedFired).toBe(true);
  });

  it("is able to read from two streams on separate connections to one database while interleaving", async () => {
    const connection1 = new Connection(db);
    const connection2 = new Connection(db);
    const rs1 = await connection1.execute<any>(query, executeOptions);
    const rs2 = await connection2.execute<any>(query, executeOptions);

    const elements1 = await readStream(rs1);
    expect(elements1.length).toBe(60);

    const elements2 = await readStream(rs2);
    expect(elements2.length).toBe(60);
  });

  // TODO: We probably want to stop queries when we close connection?
  it("is able to close - in prorgess queries run to the end", async () => {
    const query1 = "CREATE TABLE test (a INTEGER, b INTEGER);";
    const query2 =
      "INSERT INTO test SELECT a, b FROM (VALUES (11, 22), (13, 22), (12, 21)) tbl1(a,b), repeat(0, 5000) tbl2(c)";
    await connection.execute<any>(query1, executeOptions);
    const p = connection.execute<any>(query2, executeOptions);
    connection.close();
    const elements = await readStream(await p);
    expect(elements).toEqual([[BigInt(15000)]]);
  });
});
