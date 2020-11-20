import { DuckDB, Connection, } from "@addon";
import { RowResultFormat } from "@addon-types";

describe("Data type mapping", () => {
    let db: DuckDB;
    let connection: Connection;
    beforeEach(() => {
      db = new DuckDB();
      connection = new Connection(db);
    });
  
    afterEach(() => {
      connection.close();
      db.close();
    });
  
    it("correctly resolves HUGEINT", async () => {
        const result = await connection.executeIterator(
            `SELECT path1, count(url), avg(deeprank), sum(links_in_count), sum(backlink_count), max(folder_count) FROM parquet_scan('crawl_urls.parquet') WHERE ((url <> '' AND url IS NOT NULL) AND ((css <> TRUE OR css IS NULL) AND (js <> TRUE OR js IS NULL) AND (is_image <> TRUE OR is_image IS NULL) AND internal = TRUE)) GROUP BY path1 ORDER BY count(url) DESC LIMIT 10`,
          );
          expect(result.fetchRow()).toMatchObject({"avg(deeprank)": 0.27228960482907333, "count(url)": 99729, "max(folder_count)": 12, "path1": "www.theverge.com", "sum(backlink_count)": null, "sum(links_in_count)": 7194376});
    })

    it("char", async () => {
      const result = await connection.executeIterator(
        `SELECT 
        CAST('a' AS CHAR)
            `, { rowResultFormat: RowResultFormat.Array}
      );
  
      expect(result.fetchRow()).toMatchObject(["a"]);
    });


  it.only("can read a single record containing all types", async () => {
    const result = await connection.executeIterator(
      `SELECT 
            TIME '01:01:01.001'
          `,
          { rowResultFormat: RowResultFormat.Array},
    );

    expect(result.fetchRow()).toMatchObject([
      1 + 1000 + 60000 + 60000 * 60,
    ]);
  });
})
