import { ConnectionWrapper, ResultType } from "../index";

const query = "SELECT count(*) FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')";

describe("Streaming/materialized capability", () => {
  it("allows streaming", async () => {
    const cw = new ConnectionWrapper();
    const rw = await cw.execute(query, false);
    expect(rw.fetchRow()).toMatchObject([60]);
    expect(rw.type).toBe(ResultType.Streaming);
  });
  it("streams by default", async () => {
    const cw = new ConnectionWrapper();
    const rw = await cw.execute(query);
    expect(rw.fetchRow()).toMatchObject([60]);
    expect(rw.type).toBe(ResultType.Streaming);
  });
  it("allows materialized", async () => {
    const cw = new ConnectionWrapper();
    const rw = await cw.execute(query, true);
    expect(rw.fetchRow()).toMatchObject([60]);
    expect(rw.type).toBe(ResultType.Materialized);
  });
  it("validates type parameter", async () => {
    const cw = new ConnectionWrapper();
    await expect((<any>cw).execute(query, "i break you")).rejects.toMatchObject({
      message: "Second argument is an optional boolean",
    });
  });
});
