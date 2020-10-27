import { ConnectionWrapper } from "../index";

jest.setTimeout(888888);
describe("Execute method error handling", () => {
  it("validates parameters", async () => {
    const cw = new ConnectionWrapper();
    await expect((<any>cw).execute()).rejects.toMatchObject({ message: "String expected" });
  });
  it("correctly handles an invalid query", async () => {
    const cw = new ConnectionWrapper();
    await expect(cw.execute("an invalid query")).rejects.toMatchObject({
      message: `Parser: syntax error at or near "an" [1]`,
    });
  });
  it("correctly handles a failing query - file does not exist", async () => {
    const cw = new ConnectionWrapper();
    await expect(cw.execute("SELECT * FROM read_csv_auto('/idontexist.csv')")).rejects.toMatchObject({
      message: `IO: File "/idontexist.csv" not found`,
    });
  });
});
