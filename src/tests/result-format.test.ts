import { Connection, DuckDB } from "@addon";
import { RowResultFormat } from "@addon-types";

const query = "SELECT * FROM parquet_scan('src/tests/test-fixtures/alltypes_plain.parquet')";

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

  it.only("is selected as JSON by default", async () => {
    const result = await connection.executeIterator(query);
    expect(result.fetchRow()).toEqual({});
  });

  it("can be specified explicitly as JSON", async () => {
    const result = await connection.executeIterator(query, { rowResultFormat: RowResultFormat.JSON });
    expect(result.fetchRow()).toEqual({});
  });

  it("can be specified explicitly as array", async () => {
    const result = await connection.executeIterator(query, { rowResultFormat: RowResultFormat.Array });
    expect(result.fetchRow()).toEqual({});
  });

  it("throws when the parameter is of wrong type", async () => {
    await expect(connection.executeIterator(query, <any>{ rowResultFormat: 1 })).rejects.toMatchObject({
      message: "rowResultFormat must be of RowResultFormat type",
    });
  });

  it("throws when the parameter is of wrong value", async () => {
    await expect(connection.executeIterator(query, <any>{ rowResultFormat: "invalid" })).rejects.toMatchObject({
      message: "rowResultFormat must be of RowResultFormat type",
    });
  });
});
