/* eslint-disable */
import { Connection, DuckDB } from "@addon";
import { AccessMode } from "@addon-types";

import { fileSystem } from "./node-filesystem";

/**
 * TODO: We probably can't use internal DC test data for privacy concerns, so maybe create a synthetic/sanitized data set
 * For now this is a useful way to gauge performance
 */
// eslint-disable-next-line jest/no-disabled-tests
describe.skip("Perfomance test suite", () => {
  let db: DuckDB;
  const path = "";
  beforeEach(async () => {
    db = new DuckDB({ options: { accessMode: AccessMode.ReadWrite, useDirectIO: false } }, fileSystem);
    await db.init();
    // await connection.executeIterator("PRAGMA threads=1;");
  });

  afterEach(() => {
    db.close();
  });
  it("serial", async () => {
    const connection = new Connection(db);
    await connection.executeIterator(
      `SELECT http_status_code, COUNT(url), AVG(deeprank), COUNT(links_in_count), COUNT(backlink_count) FROM parquet_scan('${path}') GROUP BY http_status_code ORDER BY http_status_code`,
    );
    await connection.executeIterator(
      `SELECT level, COUNT(url), AVG(deeprank), COUNT(links_in_count), COUNT(backlink_count) FROM parquet_scan('${path}') GROUP BY level ORDER BY level`,
    );
    await connection.executeIterator(
      `SELECT count(url) FROM parquet_scan('${path}') WHERE (http_status_code = 200 AND meta_redirect = FALSE AND primary_page = TRUE AND indexable = TRUE AND canonicalized_page = FALSE AND (paginated_page = FALSE OR (paginated_page = TRUE AND page_1 = TRUE))) AND ((css != TRUE AND js != TRUE AND is_image != TRUE AND internal = TRUE) AND (header_content_type = 'text/html' OR header_content_type = '')) ORDER BY count(url) DESC`,
    );
    await connection.executeIterator(
      `SELECT count(url) FROM parquet_scan('${path}') WHERE ((http_status_code = 200 AND meta_redirect = FALSE AND primary_page = TRUE AND indexable = TRUE AND canonicalized_page = FALSE AND (paginated_page = FALSE OR (paginated_page = TRUE AND page_1 = TRUE))) AND ((css != TRUE AND js != TRUE AND is_image != TRUE AND internal = TRUE) AND (header_content_type = 'text/html' OR header_content_type = ''))) ORDER BY count(url) DESC`,
    );
    await connection.executeIterator(
      `SELECT http_status_code, COUNT(url), AVG(deeprank), COUNT(links_in_count), COUNT(backlink_count) FROM parquet_scan('${path}') GROUP BY http_status_code ORDER BY http_status_code`,
    );
    await connection.executeIterator(
      `SELECT level, COUNT(url), AVG(deeprank), COUNT(links_in_count), COUNT(backlink_count) FROM parquet_scan('${path}') GROUP BY level ORDER BY level`,
    );
    await connection.executeIterator(
      `SELECT count(url) FROM parquet_scan('${path}') WHERE (http_status_code = 200 AND meta_redirect = FALSE AND primary_page = TRUE AND indexable = TRUE AND canonicalized_page = FALSE AND (paginated_page = FALSE OR (paginated_page = TRUE AND page_1 = TRUE))) AND ((css != TRUE AND js != TRUE AND is_image != TRUE AND internal = TRUE) AND (header_content_type = 'text/html' OR header_content_type = '')) ORDER BY count(url) DESC`,
    );
    await connection.executeIterator(
      `SELECT count(url) FROM parquet_scan('${path}') WHERE ((http_status_code = 200 AND meta_redirect = FALSE AND primary_page = TRUE AND indexable = TRUE AND canonicalized_page = FALSE AND (paginated_page = FALSE OR (paginated_page = TRUE AND page_1 = TRUE))) AND ((css != TRUE AND js != TRUE AND is_image != TRUE AND internal = TRUE) AND (header_content_type = 'text/html' OR header_content_type = ''))) ORDER BY count(url) DESC`,
    );
    await connection.executeIterator(
      `SELECT count(url) FROM parquet_scan('${path}') WHERE (http_status_code = 200 AND meta_redirect = FALSE AND primary_page = TRUE AND indexable = TRUE AND canonicalized_page = FALSE AND (paginated_page = FALSE OR (paginated_page = TRUE AND page_1 = TRUE))) AND ((css != TRUE AND js != TRUE AND is_image != TRUE AND internal = TRUE) AND (header_content_type = 'text/html' OR header_content_type = '')) ORDER BY count(url) DESC`,
    );
    await connection.executeIterator(
      `SELECT count(url) FROM parquet_scan('${path}') WHERE ((http_status_code = 200 AND meta_redirect = FALSE AND primary_page = TRUE AND indexable = TRUE AND canonicalized_page = FALSE AND (paginated_page = FALSE OR (paginated_page = TRUE AND page_1 = TRUE))) AND ((css != TRUE AND js != TRUE AND is_image != TRUE AND internal = TRUE) AND (header_content_type = 'text/html' OR header_content_type = ''))) ORDER BY count(url) DESC`,
    );
    connection.close();
  });

  // eslint-disable-next-line jest/expect-expect
  it("parallel", async () => {
    await Promise.all([
      (async () => {
        const connection = new Connection(db);
        const result = await connection.executeIterator(
          `SELECT http_status_code, COUNT(url), AVG(deeprank), COUNT(links_in_count), COUNT(backlink_count) FROM parquet_scan('${path}') GROUP BY http_status_code ORDER BY http_status_code`,
        );
        console.log(result.fetchAllRows());
        connection.close();
      })(),
      (async () => {
        const connection = new Connection(db);
        const result = await connection.executeIterator(
          `SELECT level, COUNT(url), AVG(deeprank), COUNT(links_in_count), COUNT(backlink_count) FROM parquet_scan('${path}') GROUP BY level ORDER BY level`,
        );
        console.log(result.fetchAllRows());
        connection.close();
      })(),
      (async () => {
        const connection = new Connection(db);
        const result = await connection.executeIterator(
          `SELECT count(url) FROM parquet_scan('${path}') WHERE (http_status_code = 200 AND meta_redirect = FALSE AND primary_page = TRUE AND indexable = TRUE AND canonicalized_page = FALSE AND (paginated_page = FALSE OR (paginated_page = TRUE AND page_1 = TRUE))) AND ((css != TRUE AND js != TRUE AND is_image != TRUE AND internal = TRUE) AND (header_content_type = 'text/html' OR header_content_type = '')) ORDER BY count(url) DESC`,
        );
        console.log(result.fetchAllRows());
        connection.close();
      })(),
      (async () => {
        const connection = new Connection(db);
        const result = await connection.executeIterator(
          `SELECT count(url) FROM parquet_scan('${path}') WHERE ((http_status_code = 200 AND meta_redirect = FALSE AND primary_page = TRUE AND indexable = TRUE AND canonicalized_page = FALSE AND (paginated_page = FALSE OR (paginated_page = TRUE AND page_1 = TRUE))) AND ((css != TRUE AND js != TRUE AND is_image != TRUE AND internal = TRUE) AND (header_content_type = 'text/html' OR header_content_type = ''))) ORDER BY count(url) DESC`,
        );
        console.log(result.fetchAllRows());
        connection.close();
      })(),
    ]);
  });
});
