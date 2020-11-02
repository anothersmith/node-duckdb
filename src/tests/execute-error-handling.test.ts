import { Connection, DuckDB } from "@addon";

describe("executeIterator method error handling", () => {
  let db: DuckDB;
  let connection: Connection;
  beforeEach(() => {
    db = new DuckDB();
    connection = new Connection(db);
  });

  it("validates parameters", async () => {
    await expect((<any>connection).executeIterator()).rejects.toMatchObject({
      message: "First argument must be a string",
    });
  });
  it("correctly handles an invalid query", async () => {
    await expect(connection.executeIterator("an invalid query")).rejects.toMatchObject({
      message: `Parser: syntax error at or near "an" [1]`,
    });
  });
  it("correctly handles a failing query - file does not exist", async () => {
    await expect(connection.executeIterator("SELECT * FROM read_csv_auto('/idontexist.csv')")).rejects.toMatchObject({
      message: `IO: File "/idontexist.csv" not found`,
    });
  });
});
