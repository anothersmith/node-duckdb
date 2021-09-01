import { Connection, DuckDB } from "@addon";
import { IExecuteOptions, RowResultFormat } from "@addon-types";

const executeOptions: IExecuteOptions = { rowResultFormat: RowResultFormat.Array };

describe("Http/s3 interface", () => {
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

  it("allows reading from https", async () => {
    const result = await connection.executeIterator(
      "SELECT * FROM parquet_scan('https://github.com/deepcrawl/node-duckdb/raw/master/src/tests/test-fixtures/alltypes_plain.parquet')",
      executeOptions,
    );
    expect(result.fetchRow()).toMatchObject([
      4,
      true,
      0,
      0,
      0,
      0n,
      0,
      0,
      Buffer.from("03/01/09"),
      Buffer.from("0"),
      1235865600000,
    ]);
  });
});
