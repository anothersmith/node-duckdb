import { Connection, DuckDB } from "../index";

describe("description()", () => {
  let db: DuckDB;
  let cw: Connection;
  beforeEach(() => {
    db = new DuckDB();
    cw = new Connection(db);
  });

  it("can read column names", async () => {
    const rw = await cw.executeIterator(`SELECT 
        null AS c_null,
        0,
        'something',
        'something' AS something
      `);

    expect(rw.describe()).toMatchObject([
      ["c_null", "INTEGER"],
      ["0", "INTEGER"],
      ["something", "VARCHAR"],
      ["something", "VARCHAR"],
    ]);
  });
});
