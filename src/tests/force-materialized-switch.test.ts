import { Connection, DuckDB, ResultType } from "../index";

const query = "SELECT count(*) FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')";

describe("Streaming/materialized capability", () => {
  let db: DuckDB;
  let cw: Connection;
  beforeEach(() => {
    db = new DuckDB();
    cw = new Connection(db);
  });

  it("allows streaming", async () => {
    const rw = await cw.executeIterator(query, false);
    expect(rw.fetchRow()).toMatchObject([60]);
    expect(rw.type).toBe(ResultType.Streaming);
  });
  it("streams by default", async () => {
    const rw = await cw.executeIterator(query);
    expect(rw.fetchRow()).toMatchObject([60]);
    expect(rw.type).toBe(ResultType.Streaming);
  });
  it("allows materialized", async () => {
    const rw = await cw.executeIterator(query, true);
    expect(rw.fetchRow()).toMatchObject([60]);
    expect(rw.type).toBe(ResultType.Materialized);
  });
  it("validates type parameter", async () => {
    await expect((<any>cw).executeIterator(query, "i break you")).rejects.toMatchObject({
      message: "Second argument is an optional boolean",
    });
  });
});
