/* eslint-disable jest/no-disabled-tests */
import { ConnectionWrapper, ResultWrapper } from "./addon-wrapper";

describe("node-duckdb", () => {
  it("exports a ConnectionWrapper", () => {
    expect(ConnectionWrapper).not.toBe(undefined);
  });

  describe("ConnectionWrapper", () => {
    it("can be instantiated", () => {
      const cw = new ConnectionWrapper();

      expect(cw).toBeInstanceOf(ConnectionWrapper);
    });

    describe("execute()", () => {
      it("validates parameters", async () => {
        const cw = new ConnectionWrapper();
        await expect((<any>cw).execute()).rejects.toEqual(TypeError("String expected"));
      });

      it("returns a ResultWrapper", async () => {
        const cw = new ConnectionWrapper();

        const rw = await cw.execute("SELECT 1");

        expect(rw).toBeDefined();
        expect(rw).toBeInstanceOf(ResultWrapper);
      });

      it("can do a csv scan - count", async () => {
        const cw = new ConnectionWrapper();

        const rw = await cw.execute("SELECT count(*) FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')");
        expect(rw.fetchRow()).toMatchObject([60]);
        expect(rw.fetchRow()).toBe(null);
      });

      it("can do a csv scan - select all", async () => {
        const cw = new ConnectionWrapper();

        const rw = await cw.execute("SELECT * FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')");
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

      it("can do concurrent operations with same ConnectionWrapper", async () => {
        const cw = new ConnectionWrapper();
        const p1 = cw.execute("SELECT * FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')");

        const p2 = cw.execute("SELECT count(*) FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')");
        const [rw1, rw2] = await Promise.all([p1, p2])
        expect(rw1.fetchRow()).toMatchObject([
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

        expect(rw2.fetchRow()).toMatchObject([60]);
      });


      it("can do concurrent operations with different ConnectionWrapper", async () => {
        const cw1 = new ConnectionWrapper();
        const cw2 = new ConnectionWrapper();
        const p1 = cw1.execute("SELECT * FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')");

        const p2 = cw2.execute("SELECT count(*) FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')");
        const [rw1, rw2] = await Promise.all([p1, p2])
        expect(rw1.fetchRow()).toMatchObject([
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

        expect(rw2.fetchRow()).toMatchObject([60]);
      });


      it("can do a consequtive operations", async () => {
        const cw = new ConnectionWrapper();

        const rw1 = await cw.execute("SELECT * FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')");
        expect(rw1.fetchRow()).toMatchObject([
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

        const rw2 = await cw.execute("SELECT count(*) FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')");
        expect(rw2.fetchRow()).toMatchObject([60]);
      });

      it("can do a parquet scan - count", async () => {
        const cw = new ConnectionWrapper();

        const rw = await cw.execute(
          "SELECT count(*) FROM parquet_scan('src/addon-wrapper/test-fixtures/alltypes_plain.parquet')",
        );
        expect(rw.fetchRow()).toMatchObject([8]);
        expect(rw.fetchRow()).toBe(null);
      });

      it("can do a parquet scan - select all", async () => {
        const cw = new ConnectionWrapper();

        const rw = await cw.execute("SELECT * FROM parquet_scan('src/addon-wrapper/test-fixtures/alltypes_plain.parquet')");
        expect(rw.fetchRow()).toMatchObject([4, true, 0, 0, 0, 0, 0, 0, "03/01/09", "0", 1235865600000]);
        expect(rw.fetchRow()).toMatchObject([5, false, 1, 1, 1, 10, 1.100000023841858, 10.1, "03/01/09", "1", 1235865660000]);
      });

      jest.setTimeout(60000)
      it("long running execution does not block thread", async () => {
        let lastDate = new Date();
        let didEventLoopBlock = false;
        const timer = setInterval(() => {
          const currentDate = new Date();
          // if setInterval hasn't fired within last 50 ms then assume event loop has been blocked
          if (new Date(lastDate.getTime() + 50) < currentDate) {
            didEventLoopBlock = true;
          }
          lastDate = currentDate;
        }, 0)
        const cw = new ConnectionWrapper();
        await cw.execute("CREATE TABLE test (a INTEGER, b INTEGER);");
        const operationStartTime = new Date();
        await cw.execute("INSERT INTO test SELECT a, b FROM (VALUES (11, 22), (13, 22), (12, 21)) tbl1(a,b), repeat(0, 300000000) tbl2(c)");
        const operationEndTime = new Date();
        // operation must take longer than 20 secs
        expect(new Date(operationStartTime.getTime() + 20000) < operationEndTime).toBe(true);
        expect(didEventLoopBlock).toBe(false);
        clearInterval(timer);
      });

    });
  });

  describe("ResultWrapper", () => {
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

    describe("fetchRow()", () => {
      it("errors when without a result", () => {
        const rw = new ResultWrapper();

        expect(rw).toBeInstanceOf(ResultWrapper);

        expect(() => rw.fetchRow()).toThrow("Result closed");
      });

      it("can read a single record containing all types", async () => {
        const cw = new ConnectionWrapper();
        const rw = await cw.execute(`SELECT 
          null,
          true,
          0,
          CAST(1 AS TINYINT),
          CAST(8 AS SMALLINT),
          10000,
          9223372036854775807,
          1.1,        
          CAST(1.1 AS DOUBLE),
          'stringy',
          TIMESTAMP '1971-02-02 01:01:01.001',
          DATE '1971-02-02',
          TIME '01:01:01.001'
        `);

        expect(rw.fetchRow()).toMatchObject([
          null,
          true,
          0,
          1,
          8,
          10000,
          9223372036854776000, // Note: not a BigInt (yet)
          1.1,
          1.1,
          "stringy",
          Date.UTC(71, 1, 2, 1, 1, 1, 1),
          Date.UTC(71, 1, 2),
          1 + 1000 + 60000 + 60000 * 60,
        ]);
      });
    });
  });
});
