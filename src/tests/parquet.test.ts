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
      "SELECT count(*) FROM parquet_scan('src/tests/test-fixtures/data-types.parquet')",
      executeOptions,
    );
    expect(result.fetchRow()).toMatchObject([5n]);
    expect(result.fetchRow()).toBe(null);
  });

  it("can do a select all", async () => {
    const result = await connection.executeIterator(
      "SELECT * FROM parquet_scan('src/tests/test-fixtures/data-types.parquet')",
      executeOptions,
    );
    expect(result.fetchRow()).toMatchObject([null, null, null, null, null, null, null, null, null, null, null, null]);
    expect(result.fetchRow()).toMatchObject([
      42,
      43,
      44,
      45n,
      4.599999904632568,
      4.7,
      480,
      '49',
      Buffer.from([53, 48]),
      true,
      1574799102501,
      18271
    ]);
  });

  it("handles Object output format", async () => {
    const result = await connection.executeIterator(
      "SELECT * FROM parquet_scan('src/tests/test-fixtures/data-types.parquet')",
    );
    expect(result.fetchAllRows()[1]).toEqual({
      byteval: 42,
      shortval: 43,
      integerval: 44,
      longval: 45n,
      floatval: 4.599999904632568,
      doubleval: 4.7,
      decimalval: 480,
      stringval: '49',
      binaryval: Buffer.from([53, 48]),
      booleanval: true,
      timestampval: 1574799102501,
      dateval: 18271
    });
  });
});
