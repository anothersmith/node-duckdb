import { Connection, DuckDB } from "@addon";
import { ResultType, RowResultFormat } from "@addon-types";

const query = "SELECT count(*) FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')";

describe("Streaming/materialized capability", () => {
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

  it("allows streaming", async () => {
    const result = await connection.executeIterator(query, {
      rowResultFormat: RowResultFormat.Array,
      forceMaterialized: false,
    });
    expect(result.fetchRow()).toMatchObject([BigInt(60)]);
    expect(result.type).toBe(ResultType.Streaming);
  });
  it("streams by default", async () => {
    const result = await connection.executeIterator(query, { rowResultFormat: RowResultFormat.Array });
    expect(result.fetchRow()).toMatchObject([BigInt(60)]);
    expect(result.type).toBe(ResultType.Streaming);
  });
  it("allows materialized", async () => {
    const result = await connection.executeIterator(query, {
      rowResultFormat: RowResultFormat.Array,
      forceMaterialized: true,
    });
    expect(result.fetchRow()).toMatchObject([BigInt(60)]);
    expect(result.type).toBe(ResultType.Materialized);
  });
  it("validates type parameter", async () => {
    await expect((<any>connection).executeIterator(query, "i break you")).rejects.toMatchObject({
      message: "Second argument is an optional object",
    });
  });
});
