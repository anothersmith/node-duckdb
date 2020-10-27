import { ConnectionWrapper, ResultWrapper } from "../index";

describe("description()", () => {
  it("errors when without a result", () => {
    const rw = new ResultWrapper();

    expect(rw).toBeInstanceOf(ResultWrapper);

    expect(() => rw.describe()).toThrow("Result closed");
  });

  it("can read column names", async () => {
    const cw = new ConnectionWrapper();
    const rw = await cw.execute(`SELECT 
        null AS c_null,
        0,
        'something',
        'something' AS something
      `);

    expect(rw.describe()).toMatchObject([
      ["c_null", "INTEGER"],
      ["0", "INTEGER"],
      ["something", "VARCHAR"],
      ["something", "VARCHAR"],
    ]);
  });
});
