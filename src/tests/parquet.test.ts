import { Connection, DuckDB } from "@addon";
import { IExecuteOptions, RowResultFormat } from "@addon-types";

const executeOptions: IExecuteOptions = { rowResultFormat: RowResultFormat.Array };

describe("executeIterator on parquet", () => {
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

  it("can do a count", async () => {
    const result = await connection.executeIterator(
      "SELECT count(*) FROM parquet_scan('src/tests/test-fixtures/alltypes_plain.parquet')",
      executeOptions,
    );
    expect(result.fetchRow()).toMatchObject([8n]);
    expect(result.fetchRow()).toBe(null);
  });

  it("can do a select all", async () => {
    const result = await connection.executeIterator(
      "SELECT * FROM parquet_scan('src/tests/test-fixtures/alltypes_plain.parquet')",
      executeOptions,
    );
    expect(result.fetchRow()).toMatchObject([4, true, 0, 0, 0, 0n, 0, 0, Buffer.from("03/01/09"), Buffer.from("0"), 1235865600000]);
    expect(result.fetchRow()).toMatchObject([
      5,
      false,
      1,
      1,
      1,
      10n,
      1.100000023841858,
      10.1,
      Buffer.from("03/01/09"),
      Buffer.from("1"),
      1235865660000,
    ]);
  });
});
