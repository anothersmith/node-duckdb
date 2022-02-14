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
    const result = await connection.executeIterator<number[]>(`SELECT CAST(1 AS TINYINT)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    expect(result.fetchRow()).toMatchObject([1]);
  });

  it("supports SMALLINT", async () => {
    const result = await connection.executeIterator<number[]>(`SELECT CAST(1 AS SMALLINT)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    expect(result.fetchRow()).toMatchObject([1]);
  });

  it("supports INTEGER", async () => {
    const result = await connection.executeIterator<number[]>(`SELECT CAST(1 AS INTEGER)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    expect(result.fetchRow()).toMatchObject([1]);
  });

  it("supports BIGINT", async () => {
    const bigInt = 9223372036854775807n;
    const result = await connection.executeIterator<Array<bigint>>(`SELECT CAST (${bigInt} AS BIGINT)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    const resultValue = result.fetchRow()[0];
    expect(resultValue).toEqual(bigInt);
  });

  it("supports BIGINT - negative", async () => {
    const bigInt = -1n;
    const result = await connection.executeIterator<Array<bigint>>(`SELECT CAST (${bigInt} AS BIGINT)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    const resultValue = result.fetchRow()[0];
    expect(resultValue).toEqual(bigInt);
  });

  it("supports HUGEINT - positive max", async () => {
    const hugeInt = 170141183460469231731687303715884105727n;
    const result = await connection.executeIterator<Array<bigint>>(`SELECT CAST (${hugeInt} AS HUGEINT)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    const resultValue = result.fetchRow()[0];
    expect(resultValue).toEqual(hugeInt);
  });

  it("supports HUGEINT - large negative", async () => {
    const hugeInt = -4565365654654345325455654365n;
    const result = await connection.executeIterator<Array<bigint>>(`SELECT CAST (${hugeInt} AS HUGEINT)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    const resultValue = result.fetchRow()[0];
    expect(resultValue).toEqual(hugeInt);
  });

  it("supports HUGEINT - small positive number", async () => {
    const hugeInt = 132142n;
    const result = await connection.executeIterator<Array<bigint>>(`SELECT CAST (${hugeInt} AS HUGEINT)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    const resultValue = result.fetchRow()[0];
    expect(resultValue).toEqual(hugeInt);
  });

  it("supports HUGEINT - negative 1", async () => {
    const hugeInt = -1n;
    const result = await connection.executeIterator<Array<bigint>>(`SELECT CAST (${hugeInt} AS HUGEINT)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    const resultValue = result.fetchRow()[0];
    expect(resultValue).toEqual(hugeInt);
  });

  it("supports HUGEINT - large negative number", async () => {
    const hugeInt = -354235423543264236543654n;
    const result = await connection.executeIterator<Array<bigint>>(`SELECT CAST (${hugeInt} AS HUGEINT)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    const resultValue = result.fetchRow()[0];
    expect(resultValue).toEqual(hugeInt);
  });

  it("supports HUGEINT - negative max", async () => {
    const hugeInt = -170141183460469231731687303715884105727n;
    const result = await connection.executeIterator<Array<bigint>>(`SELECT CAST (${hugeInt} AS HUGEINT)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    const resultValue = result.fetchRow()[0];
    expect(resultValue).toEqual(hugeInt);
  });

  it("supports common types", async () => {
    const result = await connection.executeIterator<any[]>(
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
              TIMESTAMP '1971-02-02 01:01:01.001'
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
    ]);
  });

  it("supports DATE", async () => {
    const result = await connection.executeIterator<any[]>(`SELECT DATE '2000-05-05'`, {
      rowResultFormat: RowResultFormat.Array,
    });

    expect(result.fetchRow()).toEqual(["2000-05-05"]);
  });

  it("supports VARCHAR", async () => {
    const result = await connection.executeIterator<string[]>(`SELECT CAST('a' AS VARCHAR)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    expect(result.fetchRow()).toMatchObject(["a"]);
  });

  it("supports CHAR", async () => {
    const result = await connection.executeIterator<string[]>(`SELECT CAST('a' AS CHAR)`, {
      rowResultFormat: RowResultFormat.Array,
    });
    expect(result.fetchRow()).toMatchObject(["a"]);
  });

  it("supports TIME", async () => {
    const result = await connection.executeIterator<number[]>(`SELECT TIME '01:01:01.001'`, {
      rowResultFormat: RowResultFormat.Array,
    });
    expect(result.fetchRow()).toMatchObject([(1 + 1000 + 60 * 1000 + 60 * 1000 * 60) * 1000]);
  });

  it("supports BLOB", async () => {
    const result = await connection.executeIterator<Buffer[]>(`SELECT 'AB'::BLOB;`, {
      rowResultFormat: RowResultFormat.Array,
    });
    const resultBuffer = result.fetchRow()[0];
    const view = new Int8Array(resultBuffer);
    // ASCII "a"
    expect(view[0]).toBe(65);
    // ASCII "b"
    expect(view[1]).toBe(66);
  });

  // TODO: either create a JS/TS object representing an interval or possibly convert to number
  it("supports INTERVAL", async () => {
    const result = await connection.executeIterator<string[]>(`SELECT INTERVAL '1' MONTH;`, {
      rowResultFormat: RowResultFormat.Array,
    });
    expect(result.fetchRow()).toMatchObject(["1 month"]);
  });

  it("supports UTINYINT", async () => {
    const result = await connection.executeIterator<any[]>(`SELECT CAST(1 AS UTINYINT)`, {
      rowResultFormat: RowResultFormat.Array,
    });

    expect(result.fetchRow()).toMatchObject([1]);
  });

  it("supports USMALLINT", async () => {
    const result = await connection.executeIterator<any[]>(`SELECT CAST(1 AS USMALLINT)`, {
      rowResultFormat: RowResultFormat.Array,
    });

    expect(result.fetchRow()).toMatchObject([1]);
  });

  it("supports UINTEGER", async () => {
    const result = await connection.executeIterator<any[]>(`SELECT CAST(1 AS UINTEGER)`, {
      rowResultFormat: RowResultFormat.Array,
    });

    expect(result.fetchRow()).toMatchObject([1]);
  });

  it("supports LIST - integers", async () => {
    const result = await connection.executeIterator<any[]>(`SELECT array_slice([1,2,3], 1, NULL)`);
    expect(result.fetchRow()).toMatchObject({ "array_slice(list_value(1, 2, 3), 1, NULL)": [2, 3] });
  });

  it("supports LIST - strings", async () => {
    const result = await connection.executeIterator<any[]>(`SELECT array_slice(['a','b','c'], 1, NULL)`);
    expect(result.fetchRow()).toEqual({ "array_slice(list_value(a, b, c), 1, NULL)": ["b", "c"] });
  });

  it("supports LIST - STRUCTs", async () => {
    const result = await connection.executeIterator<any[]>(
      `SELECT raw_header from parquet_scan('src/tests/test-fixtures/crawl_urls.parquet')`,
    );
    const row = result.fetchRow();
    console.log("*** result of fetchRow: ", row);
    expect(row).toEqual({
      raw_header:
        "{Content-Encoding=gzip, X-Frame-Options=SAMEORIGIN, Connection=keep-alive, X-Xss-Protection=1; mode=block, Content-Type=text/html;charset=utf-8, Date=Tue, 18 Aug 2020 13:46:36 GMT, Vary=User-agent,Accept-Encoding, Server=nginx/1.10.3, X-Content-Type-Options=nosniff, Content-Length=1180}",
    });
  });

  it("supports STRUCT", async () => {
    const result = await connection.executeIterator<any[]>(`SELECT struct_pack(i := 4, s := 'string')`);
    expect(result.fetchRow()).toEqual({ "struct_pack(4, string)": { i: 4, s: "string" } });
  });
});
