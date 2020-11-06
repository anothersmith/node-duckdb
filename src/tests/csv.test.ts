import { DuckDB, Connection } from "@addon";
import { IExecuteOptions, RowResultFormat } from "@addon-types";

const executeOptions: IExecuteOptions = { rowResultFormat: RowResultFormat.Array };

describe("executeIterator on csv", () => {
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

  it("can do a count", async () => {
    const result = await connection.executeIterator(
      "SELECT count(*) FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')",
      executeOptions,
    );
    expect(result.fetchRow()).toMatchObject([60]);
    expect(result.fetchRow()).toBe(null);
  });

  it("can do a select all", async () => {
    const result = await connection.executeIterator(
      "SELECT * FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')",
      executeOptions,
    );
    expect(result.fetchRow()).toMatchObject([
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
