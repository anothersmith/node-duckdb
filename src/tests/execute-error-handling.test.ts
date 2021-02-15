import { Connection, DuckDB } from "@addon";

const invalidQueryError = `Parser Error: syntax error at or near "an"
LINE 1: an invalid query
        ^`;

describe("executeIterator method error handling", () => {
  let db: DuckDB;
  let connection: Connection;
  beforeEach(async () => {
    db = new DuckDB();
    await db.init();
    connection = new Connection(db);
  });

  afterEach(() => {
    connection.close();
    db.close();
  });

  it("validates parameters", async () => {
    await expect((<any>connection).executeIterator()).rejects.toMatchObject({
      message: "First argument must be a string",
    });
  });
  it("correctly handles an invalid query", async () => {
    await expect(connection.executeIterator("an invalid query")).rejects.toMatchObject({
      message: invalidQueryError,
    });
  });
  it("correctly handles a failing query - file does not exist", async () => {
    await expect(connection.executeIterator("SELECT * FROM read_csv_auto('/idontexist.csv')")).rejects.toMatchObject({
      message: `IO Error: No files found that match the pattern "/idontexist.csv"`,
    });
  });
});
