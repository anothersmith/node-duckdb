import { Connection, DuckDB } from "@addon";

const jsonResult = {
  bigint_col: 0,
  bool_col: true,
  date_string_col: "03/01/09",
  double_col: 0,
  float_col: 0,
  id: 4,
  int_col: 0,
  smallint_col: 0,
  string_col: "0",
  timestamp_col: 1235865600000,
  tinyint_col: 0,
};

describe("Result iterator", () => {
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

  it("allows to fetch all rows", async () => {
    const result = await connection.executeIterator(
      "SELECT * FROM parquet_scan('src/tests/test-fixtures/alltypes_plain.parquet')",
    );
    const allRows = result.fetchAllRows();
    expect(allRows[0]).toEqual(jsonResult);
    expect(allRows.length).toBe(8);
  });
});
