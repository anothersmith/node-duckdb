import { Connection, DuckDB } from "@addon";

describe("description()", () => {
  let db: DuckDB;
  let connection: Connection;
  beforeEach(() => {
    db = new DuckDB();
    connection = new Connection(db);
  });

  it("can read column names", async () => {
    const result = await connection.executeIterator(`SELECT 
        null AS c_null,
        0,
        'something',
        'something' AS something
      `);

    expect(result.describe()).toMatchObject([
      ["c_null", "INTEGER"],
      ["0", "INTEGER"],
      ["something", "VARCHAR"],
      ["something", "VARCHAR"],
    ]);
  });
});
