import { Connection, DuckDB } from "@addon";

const query1 = "SELECT * FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')";
const query2 = "SELECT count(*) FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')";
const expectedResult1 = [
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
const expectedResult2 = [60];

describe("Async executeIterator", () => {
  let db: DuckDB;
  beforeEach(() => {
    db = new DuckDB();
  });

  it("can do concurrent operations with same Connection", async () => {
    const connection = new Connection(db);
    const p1 = connection.executeIterator(query1, true);
    const p2 = connection.executeIterator(query2, true);
    const [result1, result2] = await Promise.all([p1, p2]);
    expect(result1.fetchRow()).toMatchObject(expectedResult1);
    expect(result2.fetchRow()).toMatchObject(expectedResult2);
  });

  it("can do concurrent operations with different Connection", async () => {
    const connection1 = new Connection(db);
    const connection2 = new Connection(db);
    const p1 = connection1.executeIterator(query1, true);
    const p2 = connection2.executeIterator(query2, true);
    const [result1, result2] = await Promise.all([p1, p2]);
    expect(result1.fetchRow()).toMatchObject(expectedResult1);
    expect(result2.fetchRow()).toMatchObject(expectedResult2);
  });

  it("can do a consequtive operations", async () => {
    const connection = new Connection(db);
    const result1 = await connection.executeIterator(query1, true);
    expect(result1.fetchRow()).toMatchObject(expectedResult1);
    const result2 = await connection.executeIterator(query2, true);
    expect(result2.fetchRow()).toMatchObject(expectedResult2);
  });

  jest.setTimeout(60000 * 5);
  // this test is a bit tricky to run on machines of very different specs
  it("does not block thread during a long running execution", async () => {
    const connection = new Connection(db);
    await connection.executeIterator("CREATE TABLE test (a INTEGER, b INTEGER);", true);
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
    await connection.executeIterator(
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
