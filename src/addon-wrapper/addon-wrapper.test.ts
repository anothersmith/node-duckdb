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
      it("validates parameters", () => {
        const cw = new ConnectionWrapper();

        expect(() => (<any>cw).execute()).toThrow("String expected");
      });

      it("returns a ResultWrapper", () => {
        const cw = new ConnectionWrapper();

        const rw = cw.execute("SELECT 1");

        expect(rw).toBeDefined();
        expect(rw).toBeInstanceOf(ResultWrapper);
      });

      it("can do a csv scan - count", () => {
        const cw = new ConnectionWrapper();

        const rw = cw.execute(
          "SELECT count(*) FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')",
        );
        expect(rw.fetchRow()).toMatchObject([60]);
        expect(rw.fetchRow()).toBe(null);
      });

      it("can do a csv scan - select all", () => {
        const cw = new ConnectionWrapper();

        const rw = cw.execute(
          "SELECT * FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')",
        );
        expect(rw.fetchRow()).toMatchObject([1,
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
          4,]);
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

      it("can read column names", () => {
        const cw = new ConnectionWrapper();
        const rw = cw.execute(`SELECT 
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

      it("can read a single record containing all types", () => {
        const cw = new ConnectionWrapper();
        const rw = cw.execute(`SELECT 
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
