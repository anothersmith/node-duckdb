import { ConnectionWrapper } from "../index";
import { ResultStream } from "../result-stream";

const query = "SELECT * FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')";

function readStream(rs: ResultStream): Promise<any[]> {
  return new Promise((resolve, reject) => {
    const elements: any[] = [];
    rs.on("data", (el: any) => elements.push(el));
    rs.on("error", reject);
    rs.on("end", () => resolve(elements));
  });
}

describe("Result stream", () => {
  it("reads a csv", async () => {
    const cw = new ConnectionWrapper();
    const rs = await cw.execute(query);
    const elements = await readStream(rs);
    expect(elements.length).toBe(60);
    expect(elements[0]).toEqual([
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

  it("is able to read from two streams sequentially", async () => {
    const cw = new ConnectionWrapper();
    const rs1 = await cw.execute(query);
    const elements1 = await readStream(rs1);
    expect(elements1.length).toBe(60);

    const rs2 = await cw.execute(query);
    const elements2 = await readStream(rs2);
    expect(elements2.length).toBe(60);
  });

  it("correctly handles errors - closes resource", async () => {
    const cw = new ConnectionWrapper();
    const rs1 = await cw.execute(query);
    await cw.execute(query);
    let hasClosedFired = false;
    rs1.on("close", () => hasClosedFired = true)
    await expect(readStream(rs1)).rejects.toMatchObject({
      message:
        "No data has been returned (possibly stream has been closed: only one stream can be active on one connection at a time)",
    });
    expect(hasClosedFired).toBe(true);
  });

  it("closes resource when all data has been read", async () => {
    const cw = new ConnectionWrapper();
    const rs = await cw.execute(query);
    let hasClosedFired = false;
    rs.on("close", () => hasClosedFired = true)
    const elements = await readStream(rs);
    expect(elements.length).toBe(60);
    expect(hasClosedFired).toBe(true);
  });

  it("closes resource on manual destroy", async () => {
    const cw = new ConnectionWrapper();
    const rs1 = await cw.execute(query);
    let hasClosedFired = false;
    rs1.on("close", () => hasClosedFired = true)
    void readStream(rs1);
    expect(hasClosedFired).toBe(false);
    rs1.destroy();
    expect(hasClosedFired).toBe(true);
  });
});
