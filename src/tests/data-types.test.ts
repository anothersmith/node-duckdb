import { DuckDB, Connection } from "@addon";
import { RowResultFormat } from "@addon-types";

/**
 * There are types in the source code that there is no documentation for and I'm not sure if they are used as return types:
 * - VARBINARY
 * - POINTER
 * - HASH
 * - STRUCT
 * - LIST
 * These are not supported (yet) by the bindings, not sure if they need to be
 */

describe("Data type mapping", () => {
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

  it("supports TINYINT", async () => {
    const result = await connection.executeIterator(`SELECT CAST(1 AS TINYINT)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    expect(result.fetchRow()).toMatchObject([1]);
  });

  it("supports SMALLINT", async () => {
    const result = await connection.executeIterator(`SELECT CAST(1 AS SMALLINT)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    expect(result.fetchRow()).toMatchObject([1]);
  });

  it("supports INTEGER", async () => {
    const result = await connection.executeIterator(`SELECT CAST(1 AS INTEGER)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    expect(result.fetchRow()).toMatchObject([1]);
  });

  // TODO: possibly return as BigInt (napi v5+)
  it("supports BIGINT", async () => {
    const bigInt = "9223372036854775807";

    const result = await connection.executeIterator(`SELECT CAST (${bigInt} AS BIGINT)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    const resultValue = (<number[]>result.fetchRow())[0];
    expect(resultValue).toEqual(bigInt);
  });

  // TODO: possibly return as BigInt (napi v5+)
  it("supports HUGEINT", async () => {
    const hugeInt = "-170141183460469231731687303715884105727";
    const result = await connection.executeIterator(`SELECT CAST (${hugeInt} AS HUGEINT)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    const resultValue = (<number[]>result.fetchRow())[0];
    expect(resultValue).toEqual(hugeInt);
  });

  it("supports common types", async () => {
    const result = await connection.executeIterator(
      `SELECT 
              null,
              true,
              0,
              CAST(1 AS TINYINT),
              CAST(8 AS SMALLINT),
              10000,
              1.1,        
              CAST(1.1 AS DOUBLE),
              'stringy',
              TIMESTAMP '1971-02-02 01:01:01.001',
              DATE '1971-02-02'
            `,
      { rowResultFormat: RowResultFormat.Array },
    );

    expect(result.fetchRow()).toMatchObject([
      null,
      true,
      0,
      1,
      8,
      10000,
      1.1,
      1.1,
      "stringy",
      Date.UTC(71, 1, 2, 1, 1, 1, 1),
      Date.UTC(71, 1, 2),
    ]);
  });

  // Note: even though there is a CHAR type in the source code, seems to be an alias to VARCHAR
  it("supports CHAR", async () => {
    const result = await connection.executeIterator(`SELECT CAST('a' AS CHAR)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    expect(result.fetchRow()).toMatchObject(["a"]);
  });

  it("supports TIME", async () => {
    const result = await connection.executeIterator(`SELECT TIME '01:01:01.001'`, {
      rowResultFormat: RowResultFormat.Array,
    });
    expect(result.fetchRow()).toMatchObject([1 + 1000 + 60000 + 60000 * 60]);
  });

  // TODO: use typed arrays or blobs?
  it("supports BLOB", async () => {
    const result = await connection.executeIterator(`SELECT CAST('\\x3131' AS BLOB)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    expect(result.fetchRow()).toMatchObject(["\\x3131"]);
  });

  // TODO: either create a JS/TS object representing an interval or possibly convert to number
  it("supports INTERVAL", async () => {
    const result = await connection.executeIterator(`SELECT INTERVAL '1' MONTH;`, {
      rowResultFormat: RowResultFormat.Array,
    });
    expect(result.fetchRow()).toMatchObject(["1 month"]);
  });
});
