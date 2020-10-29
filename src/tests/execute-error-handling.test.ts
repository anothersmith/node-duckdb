import { ConnectionWrapper } from "../index";

jest.setTimeout(888888);
describe("executeIterator method error handling", () => {
  it("validates parameters", async () => {
    const cw = new ConnectionWrapper();
    await expect((<any>cw).executeIterator()).rejects.toMatchObject({ message: "First argument must be a string" });
  });
  it("correctly handles an invalid query", async () => {
    const cw = new ConnectionWrapper();
    await expect(cw.executeIterator("an invalid query")).rejects.toMatchObject({
      message: `Parser: syntax error at or near "an" [1]`,
    });
  });
  it("correctly handles a failing query - file does not exist", async () => {
    const cw = new ConnectionWrapper();
    await expect(cw.executeIterator("SELECT * FROM read_csv_auto('/idontexist.csv')")).rejects.toMatchObject({
      message: `IO: File "/idontexist.csv" not found`,
    });
  });
});
