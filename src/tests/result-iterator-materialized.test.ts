import { Connection, DuckDB } from "@addon";
import { IExecuteOptions, ResultType, RowResultFormat } from "@addon-types";

const executeOptions: IExecuteOptions = { rowResultFormat: RowResultFormat.Array, forceMaterialized: true };

describe("Result iterator (materialized)", () => {
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

  it("can read a meterialized result", async () => {
    const result = await connection.executeIterator(`SELECT null`, executeOptions);
    expect(result.type).toBe(ResultType.Materialized);

    expect(result.fetchRow()).toMatchObject([null]);
  });

  it("is able to close - throws error when reading from closed result", async () => {
    const result1 = await connection.executeIterator(
      "SELECT * FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')",
      executeOptions,
    );
    expect(result1.type).toBe(ResultType.Materialized);
    expect(result1.fetchRow()).toBeTruthy();
    result1.close();
    expect(result1.isClosed).toBe(true);
    expect(() => result1.fetchRow()).toThrow("Result closed");
  });
});
