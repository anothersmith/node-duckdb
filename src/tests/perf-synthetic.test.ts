/* eslint-disable no-console, jest/expect-expect */
import { join } from "path";

import { Connection, DuckDB } from "@addon";

import { writeSyntheticParquetFile } from "./synthetic-test-data-generator";

const filePath = join(__dirname, "../large-synth.parquet");

/**
 * Test suite used to measure performance, change the number of rows argument of writeSyntheticParquetFile to increase test data set size
 */
jest.setTimeout(60000000);
describe("Perfomance test suite against synthetic data set", () => {
  let db: DuckDB;
  let connection: Connection;

  beforeAll(async () => {
    // 1000000 iterations => uncompressed 1.9 GB
    // 10000000 => 21GB/compressed 10GB
    // 10GB compressed takes around 5.5 hours to generate on my laptop
    await writeSyntheticParquetFile(filePath, 1000, true);
  });

  beforeEach(async () => {
    db = new DuckDB();
    await db.init();
    connection = new Connection(db);
    await connection.executeIterator("PRAGMA threads=4;");
  });

  afterEach(() => {
    connection.close();
    db.close();
  });

  it("multiple queries", async () => {
    await Promise.all([
      (async () => {
        const connection = new Connection(db);
        const result = await connection.executeIterator(
          `SELECT col10, COUNT(col11), AVG(col2), COUNT(col1), COUNT(col0) FROM parquet_scan('${filePath}') GROUP BY col10 ORDER BY col10`,
        );
        console.log(result.fetchAllRows());
      })(),
      (async () => {
        const connection = new Connection(db);
        const result = await connection.executeIterator(
          `SELECT col0, COUNT(col1), AVG(col2), COUNT(col0), COUNT(col3) FROM parquet_scan('${filePath}') GROUP BY col0 ORDER BY col0`,
        );
        console.log(result.fetchAllRows());
      })(),
      (async () => {
        const connection = new Connection(db);
        const result = await connection.executeIterator(
          `SELECT count(col3) FROM parquet_scan('${filePath}') WHERE (col0 > 200 AND col4 = FALSE AND col5 = TRUE AND col6 = TRUE AND col7 = FALSE AND (col8 = FALSE OR (col9 = TRUE AND col10 = TRUE))) AND ((col11 != TRUE AND col12 != TRUE AND col13 != TRUE AND col14 = TRUE) AND (col3 = 'text/html' OR col3 = '')) ORDER BY count(col3) DESC`,
        );
        console.log(result.fetchAllRows());
      })(),
      (async () => {
        const connection = new Connection(db);
        const result = await connection.executeIterator(
          `SELECT count(col0) FROM parquet_scan('${filePath}') WHERE ((col0 = 200 AND col5 = FALSE AND col6 = TRUE AND col7 = TRUE AND col8 = FALSE AND (col9 = FALSE OR (col10 = TRUE AND col11 = TRUE))) AND ((col12 != TRUE AND col13 != TRUE AND col14 != TRUE AND col15 = TRUE) AND (col3 = 'text/html' OR col3 = ''))) ORDER BY count(col0) DESC`,
        );
        console.log(result.fetchAllRows());
      })(),
    ]);
  });
});
