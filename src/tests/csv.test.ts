import { ConnectionWrapper } from "../index";

describe("Execute on csv", () => {
  it("can do a count", async () => {
    const cw = new ConnectionWrapper();

    const rw = await cw.execute("SELECT count(*) FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')");
    expect(rw.fetchRow()).toMatchObject([60]);
    expect(rw.fetchRow()).toBe(null);
  });

  it("can do a select all", async () => {
    const cw = new ConnectionWrapper();

    const rw = await cw.execute("SELECT * FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')");
    expect(rw.fetchRow()).toMatchObject([
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
