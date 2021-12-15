import { Connection, DuckDB } from "@addon";

describe("executeIterator method error handling", () => {
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

  it("validates parameters", async () => {
    await expect((<any>connection).executeIterator()).rejects.toMatchObject({
      message: "First argument must be a string",
    });
  });
  it("correctly handles an invalid query", async () => {
    let error: Error;
    try {
      await connection.executeIterator("an invalid query");
    } catch (e) {
      error = <Error>e;
    }
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    expect(error!.message).toContain("an invalid query");
  });
  it("correctly handles a failing query - file does not exist", async () => {
    await expect(connection.executeIterator("SELECT * FROM read_csv_auto('/idontexist.csv')")).rejects.toMatchObject({
      message: `IO Error: No files found that match the pattern "/idontexist.csv"`,
    });
  });
});
