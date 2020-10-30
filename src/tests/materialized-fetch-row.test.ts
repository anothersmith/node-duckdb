import { ResultWrapper, ConnectionWrapper, ResultType } from "../index";

describe("Materialized fetchRow()", () => {
  it("errors when without a result", () => {
    const rw = new ResultWrapper();

    expect(rw).toBeInstanceOf(ResultWrapper);

    expect(() => rw.fetchRow()).toThrow("Result closed");
  });

  it("can read a single record containing all types", async () => {
    const cw = new ConnectionWrapper();
    const rw = await cw.executeIterator(`SELECT 
            null,
            true,
            0,
            CAST(1 AS TINYINT),
            CAST(8 AS SMALLINT),
            10000,
            9223372036854775807,
            1.1,        
            CAST(1.1 AS DOUBLE),
            'stringy',
            TIMESTAMP '1971-02-02 01:01:01.001',
            DATE '1971-02-02',
            TIME '01:01:01.001'
          `, true);
    expect(rw.type).toBe(ResultType.Materialized);

    expect(rw.fetchRow()).toMatchObject([
      null,
      true,
      0,
      1,
      8,
      10000,
      9223372036854776000, // Note: not a BigInt (yet)
      1.1,
      1.1,
      "stringy",
      Date.UTC(71, 1, 2, 1, 1, 1, 1),
      Date.UTC(71, 1, 2),
      1 + 1000 + 60000 + 60000 * 60,
    ]);
  });

  it("is able to close before reading all results", async () => {
    const cw = new ConnectionWrapper();
    const rw1 = await cw.executeIterator("SELECT * FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')", true);
    expect(rw1.type).toBe(ResultType.Materialized);
    expect(rw1.fetchRow()).toBeTruthy();
    rw1.close();
    expect(rw1.isClosed).toBe(true);
    expect(() => rw1.fetchRow()).toThrow("Result closed");
  });
});
