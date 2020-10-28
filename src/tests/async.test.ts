import { ConnectionWrapper } from "index";

const query1 = "SELECT * FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')";
const query2 = "SELECT count(*) FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')";
const result1 = [
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
];
const result2 = [60];

describe("Async execute", () => {
  it("can do concurrent operations with same ConnectionWrapper", async () => {
    const cw = new ConnectionWrapper();
    const p1 = cw.execute(query1);
    const p2 = cw.execute(query2);
    const [rw1, rw2] = await Promise.all([p1, p2]);
    expect(rw1.fetchRow()).toMatchObject(result1);
    expect(rw2.fetchRow()).toMatchObject(result2);
  });

  it("can do concurrent operations with different ConnectionWrapper", async () => {
    const cw1 = new ConnectionWrapper();
    const cw2 = new ConnectionWrapper();
    const p1 = cw1.execute(query1);
    const p2 = cw2.execute(query2);
    const [rw1, rw2] = await Promise.all([p1, p2]);
    expect(rw1.fetchRow()).toMatchObject(result1);
    expect(rw2.fetchRow()).toMatchObject(result2);
  });

  it("can do a consequtive operations", async () => {
    const cw = new ConnectionWrapper();
    const rw1 = await cw.execute(query1);
    expect(rw1.fetchRow()).toMatchObject(result1);
    const rw2 = await cw.execute(query2);
    expect(rw2.fetchRow()).toMatchObject(result2);
  });

  jest.setTimeout(60000 * 5);
  it("does not block thread during a long running execution", async () => {
    let lastDate = new Date();
    let didEventLoopBlock = false;
    const timer = setInterval(() => {
      const currentDate = new Date();
      // if setInterval hasn't fired within last 800 ms then assume event loop has been blocked
      if (new Date(lastDate.getTime() + 800) < currentDate) {
        didEventLoopBlock = true;
      }
      lastDate = currentDate;
    }, 0);
    const cw = new ConnectionWrapper();
    await cw.execute("CREATE TABLE test (a INTEGER, b INTEGER);");
    const operationStartTime = new Date();
    await cw.execute(
      "INSERT INTO test SELECT a, b FROM (VALUES (11, 22), (13, 22), (12, 21)) tbl1(a,b), repeat(0, 80000000) tbl2(c)",
    );
    const operationEndTime = new Date();
    // operation must take longer than 5 secs
    expect(new Date(operationStartTime.getTime() + 5000) < operationEndTime).toBe(true);
    expect(didEventLoopBlock).toBe(false);
    clearInterval(timer);
  });
});
