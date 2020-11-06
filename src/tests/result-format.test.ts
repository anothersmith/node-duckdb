/* eslint-disable @typescript-eslint/naming-convention */
import { Connection, DuckDB } from "@addon";
import { RowResultFormat } from "@addon-types";

const query = "SELECT * FROM parquet_scan('src/tests/test-fixtures/alltypes_plain.parquet')";
const jsonResult = {
  "bigint_col": 0,
  "bool_col": true,
  "date_string_col": "03/01/09",
  "double_col": 0,
  "float_col": 0,
  "id": 4,
  "int_col": 0,
  "smallint_col": 0,
  "string_col": "0",
  "timestamp_col": 1235865600000,
  "tinyint_col": 0,
  };

const arrayResult = [4, true, 0, 0, 0, 0, 0, 0, "03/01/09", "0", 1235865600000];
describe("Result format", () => {
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

  it("is selected as JSON by default", async () => {
    const result = await connection.executeIterator(query);
    expect(result.fetchRow()).toEqual(jsonResult);
  });

  it("can be specified explicitly as JSON", async () => {
    const result = await connection.executeIterator(query, { rowResultFormat: RowResultFormat.JSON });
    expect(result.fetchRow()).toEqual(jsonResult);
  });

  it("can be specified explicitly as array", async () => {
    const result = await connection.executeIterator(query, { rowResultFormat: RowResultFormat.Array });
    expect(result.fetchRow()).toEqual(arrayResult);
  });

  it("throws when the parameter is of wrong type", async () => {
    await expect(connection.executeIterator(query, <any>{ rowResultFormat: 10 })).rejects.toMatchObject({
      message: "Invalid rowResultFormat: must be of appropriate enum type",
    });
  });

  it("throws when the parameter is of wrong value", async () => {
    await expect(connection.executeIterator(query, <any>{ rowResultFormat: "invalid" })).rejects.toMatchObject({
      message: "Invalid rowResultFormat: must be of appropriate enum type",
    });
  });
});
