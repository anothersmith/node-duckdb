import { ConnectionWrapper, DuckDB, DuckDBClass } from "../index";

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


describe("Async executeIterator", () => {
  let db: DuckDBClass;
  beforeEach(() => {
    db = new DuckDB();
  });

  it("can do concurrent operations with same ConnectionWrapper", async () => {
    const cw = new ConnectionWrapper(db);
    const p1 = cw.executeIterator(query1, true);
    const p2 = cw.executeIterator(query2, true);
    const [rw1, rw2] = await Promise.all([p1, p2]);
    expect(rw1.fetchRow()).toMatchObject(result1);
    expect(rw2.fetchRow()).toMatchObject(result2);
  });

  it("can do concurrent operations with different ConnectionWrapper", async () => {
    const cw1 = new ConnectionWrapper(db);
    const cw2 = new ConnectionWrapper(db);
    const p1 = cw1.executeIterator(query1, true);
    const p2 = cw2.executeIterator(query2, true);
    const [rw1, rw2] = await Promise.all([p1, p2]);
    expect(rw1.fetchRow()).toMatchObject(result1);
    expect(rw2.fetchRow()).toMatchObject(result2);
  });

  it("can do a consequtive operations", async () => {
    const cw = new ConnectionWrapper(db);
    const rw1 = await cw.executeIterator(query1, true);
    expect(rw1.fetchRow()).toMatchObject(result1);
    const rw2 = await cw.executeIterator(query2, true);
    expect(rw2.fetchRow()).toMatchObject(result2);
  });

  jest.setTimeout(60000 * 5);
  // this test is a bit tricky to run on machines of very different specs
  it("does not block thread during a long running execution", async () => {
    const cw = new ConnectionWrapper(db);
    await cw.executeIterator("CREATE TABLE test (a INTEGER, b INTEGER);", true);
    const operationStartTime = new Date();
    let lastDate = new Date();
    let didEventLoopBlock = false;
    const timer = setInterval(() => {
      const currentDate = new Date();
      // if setInterval hasn't fired within last 1.5 s then assume event loop has been blocked
      if (new Date(lastDate.getTime() + 1500) < currentDate) {
        didEventLoopBlock = true;
      }
      lastDate = currentDate;
    }, 0);
    await cw.executeIterator(
      "INSERT INTO test SELECT a, b FROM (VALUES (11, 22), (13, 22), (12, 21)) tbl1(a,b), repeat(0, 60000000) tbl2(c)",
      true,
    );
    const operationEndTime = new Date();
    // operation must take longer than 2 secs
    expect(new Date(operationStartTime.getTime() + 2000) < operationEndTime).toBe(true);
    expect(didEventLoopBlock).toBe(false);
    clearInterval(timer);
  });
});
