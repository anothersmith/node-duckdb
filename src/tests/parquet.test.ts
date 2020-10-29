import { ConnectionWrapper } from "../index";

describe("executeIterator on parquet", () => {
  it("can do a count", async () => {
    const cw = new ConnectionWrapper();

    const rw = await cw.executeIterator(
      "SELECT count(*) FROM parquet_scan('src/tests/test-fixtures/alltypes_plain.parquet')",
    );
    expect(rw.fetchRow()).toMatchObject([8]);
    expect(rw.fetchRow()).toBe(null);
  });

  it("can do a select all", async () => {
    const cw = new ConnectionWrapper();

    const rw = await cw.executeIterator("SELECT * FROM parquet_scan('src/tests/test-fixtures/alltypes_plain.parquet')");
    expect(rw.fetchRow()).toMatchObject([4, true, 0, 0, 0, 0, 0, 0, "03/01/09", "0", 1235865600000]);
    expect(rw.fetchRow()).toMatchObject([
      5,
      false,
      1,
      1,
      1,
      10,
      1.100000023841858,
      10.1,
      "03/01/09",
      "1",
      1235865660000,
    ]);
  });
});
