import { Connection, DuckDB } from "@addon";
import { RowResultFormat } from "@addon-types";

describe("Result iterator as well formed JS iterator/iterable", () => {
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

  it("supports for/each", async () => {
    const results = [];
    const result = await connection.executeIterator(
      "SELECT * FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')",
      { rowResultFormat: RowResultFormat.Array },
    );
    // eslint-disable-next-line no-loops/no-loops
    for (const row of result) {
      results.push(row);
    }
    expect(results[0]).toEqual([
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
    expect(results.length).toBe(60);
  });
});
