import { ConnectionWrapper, DuckDB, DuckDBClass } from "../index";


describe("executeIterator method error handling", () => {
  let db: DuckDBClass;
  let cw: ConnectionWrapper;
  beforeEach(() => {
    db = new DuckDB();
    cw = new ConnectionWrapper(db);
  });

  it("validates parameters", async () => {
    await expect((<any>cw).executeIterator()).rejects.toMatchObject({ message: "First argument must be a string" });
  });
  it("correctly handles an invalid query", async () => {
    await expect(cw.executeIterator("an invalid query")).rejects.toMatchObject({
      message: `Parser: syntax error at or near "an" [1]`,
    });
  });
  it("correctly handles a failing query - file does not exist", async () => {
    await expect(cw.executeIterator("SELECT * FROM read_csv_auto('/idontexist.csv')")).rejects.toMatchObject({
      message: `IO: File "/idontexist.csv" not found`,
    });
  });
});
