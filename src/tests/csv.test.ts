import { DuckDB, Connection } from "../index";

describe("executeIterator on csv", () => {
  let db: DuckDB;
  let cw: Connection;
  beforeEach(() => {
    db = new DuckDB();
    cw = new Connection(db);
  });

  afterEach(() => {
    // TODO: close
  });

  it("can do a count", async () => {
    const rw = await cw.executeIterator("SELECT count(*) FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')");
    expect(rw.fetchRow()).toMatchObject([60]);
    expect(rw.fetchRow()).toBe(null);
  });

  it("can do a select all", async () => {
    const rw = await cw.executeIterator("SELECT * FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')");
    expect(rw.fetchRow()).toMatchObject([
      1,
      "AAAAAAAABAAAAAAA",
      873244800000,
      null,
      2450810,
      2452620,
      "Y",
      98539,
      "http://www.foo.com",
      "welcome",
      2531,
      8,
      3,
      4,
    ]);
  });
});
