/* eslint-disable no-console */
import { Connection, DuckDB } from "@addon";

/**
 * TODO: We probably can't use internal DC test data for privacy concerns, so maybe create a synthetic/sanitized data set
 * For now this is a useful way to gauge performance
 */
// eslint-disable-next-line jest/no-disabled-tests
describe.skip("Perfomance test suite", () => {
  let db: DuckDB;
  let connection: Connection;
  beforeEach(async () => {
    db = new DuckDB();
    connection = new Connection(db);
    await connection.executeIterator("PRAGMA threads=4;");
  });

  afterEach(() => {
    connection.close();
    db.close();
  });

  it("q1", async () => {
    const result = await connection.executeIterator(
      "SELECT http_status_code, COUNT(url), AVG(deeprank), COUNT(links_in_count), COUNT(backlink_count) FROM parquet_scan('src/tests/test-fixtures/data/crawl_urls') GROUP BY http_status_code ORDER BY http_status_code",
    );
    console.log(result.fetchAllRows());
    expect(result.fetchAllRows()).toEqual([
      {
        http_status_code: 0,
        "count(url)": 104,
        "avg(deeprank)": 0.45230769230769263,
        "count(links_in_count)": 104,
        "count(backlink_count)": 0,
      },
      {
        http_status_code: 200,
        "count(url)": 82656,
        "avg(deeprank)": 0.9296001500193196,
        "count(links_in_count)": 82656,
        "count(backlink_count)": 0,
      },
      {
        http_status_code: 301,
        "count(url)": 11270,
        "avg(deeprank)": 0.8640434782608553,
        "count(links_in_count)": 11270,
        "count(backlink_count)": 0,
      },
      {
        http_status_code: 302,
        "count(url)": 2435,
        "avg(deeprank)": 0.6101026694045107,
        "count(links_in_count)": 2435,
        "count(backlink_count)": 0,
      },
      {
        http_status_code: 303,
        "count(url)": 6,
        "avg(deeprank)": 1.988333333333333,
        "count(links_in_count)": 6,
        "count(backlink_count)": 0,
      },
      {
        http_status_code: 307,
        "count(url)": 4,
        "avg(deeprank)": 0.77,
        "count(links_in_count)": 4,
        "count(backlink_count)": 0,
      },
      {
        http_status_code: 403,
        "count(url)": 360,
        "avg(deeprank)": 1.8403611111111096,
        "count(links_in_count)": 360,
        "count(backlink_count)": 0,
      },
      {
        http_status_code: 404,
        "count(url)": 1122,
        "avg(deeprank)": 0.37586452762923545,
        "count(links_in_count)": 1122,
        "count(backlink_count)": 0,
      },
      {
        http_status_code: 500,
        "count(url)": 2,
        "avg(deeprank)": 2.835,
        "count(links_in_count)": 2,
        "count(backlink_count)": 0,
      },
    ]);
  });
  it("q2", async () => {
    const result = await connection.executeIterator(
      "SELECT level, COUNT(url), AVG(deeprank), COUNT(links_in_count), COUNT(backlink_count) FROM parquet_scan('src/tests/test-fixtures/data/crawl_urls') GROUP BY level ORDER BY level",
    );
    console.log(result.fetchAllRows());
    expect(result.fetchAllRows()).toEqual([
      {
        level: 1,
        "count(url)": 21,
        "avg(deeprank)": 0.26904761904761904,
        "count(links_in_count)": 21,
        "count(backlink_count)": 0,
      },
      {
        level: 2,
        "count(url)": 1673,
        "avg(deeprank)": 3.8029348475792117,
        "count(links_in_count)": 1673,
        "count(backlink_count)": 0,
      },
      {
        level: 3,
        "count(url)": 35020,
        "avg(deeprank)": 1.2454286122216245,
        "count(links_in_count)": 35020,
        "count(backlink_count)": 0,
      },
      {
        level: 4,
        "count(url)": 61245,
        "avg(deeprank)": 0.6405339211364642,
        "count(links_in_count)": 61245,
        "count(backlink_count)": 0,
      },
    ]);
  });
  it("q3", async () => {
    const result = await connection.executeIterator(
      "SELECT count(url) FROM parquet_scan('src/tests/test-fixtures/data/crawl_urls') WHERE (http_status_code = 200 AND meta_redirect = FALSE AND primary_page = TRUE AND indexable = TRUE AND canonicalized_page = FALSE AND (paginated_page = FALSE OR (paginated_page = TRUE AND page_1 = TRUE))) AND ((css != TRUE AND js != TRUE AND is_image != TRUE AND internal = TRUE) AND (header_content_type = 'text/html' OR header_content_type = '')) ORDER BY count(url) DESC",
    );
    console.log(result.fetchAllRows());
    expect(result.fetchAllRows()).toEqual([
      {
        "count(url)": 38987,
      },
    ]);
  });
  it("q4", async () => {
    const result = await connection.executeIterator(
      "SELECT count(url) FROM parquet_scan('src/tests/test-fixtures/data/crawl_urls') WHERE ((http_status_code = 200 AND meta_redirect = FALSE AND primary_page = TRUE AND indexable = TRUE AND canonicalized_page = FALSE AND (paginated_page = FALSE OR (paginated_page = TRUE AND page_1 = TRUE))) AND ((css != TRUE AND js != TRUE AND is_image != TRUE AND internal = TRUE) AND (header_content_type = 'text/html' OR header_content_type = ''))) ORDER BY count(url) DESC",
    );
    console.log(result.fetchAllRows());
    expect(result.fetchAllRows()).toEqual([
      {
        "count(url)": 38987,
      },
    ]);
  });

  // eslint-disable-next-line jest/expect-expect
  it("q-all", async () => {
    await Promise.all([
      (async () => {
        const connection = new Connection(db);
        const result = await connection.executeIterator(
          "SELECT http_status_code, COUNT(url), AVG(deeprank), COUNT(links_in_count), COUNT(backlink_count) FROM parquet_scan('src/tests/test-fixtures/data/crawl_urls') GROUP BY http_status_code ORDER BY http_status_code",
        );
        console.log(result.fetchAllRows());
      })(),
      (async () => {
        const connection = new Connection(db);
        const result = await connection.executeIterator(
          "SELECT level, COUNT(url), AVG(deeprank), COUNT(links_in_count), COUNT(backlink_count) FROM parquet_scan('src/tests/test-fixtures/data/crawl_urls') GROUP BY level ORDER BY level",
        );
        console.log(result.fetchAllRows());
      })(),
      (async () => {
        const connection = new Connection(db);
        const result = await connection.executeIterator(
          "SELECT count(url) FROM parquet_scan('src/tests/test-fixtures/data/crawl_urls') WHERE (http_status_code = 200 AND meta_redirect = FALSE AND primary_page = TRUE AND indexable = TRUE AND canonicalized_page = FALSE AND (paginated_page = FALSE OR (paginated_page = TRUE AND page_1 = TRUE))) AND ((css != TRUE AND js != TRUE AND is_image != TRUE AND internal = TRUE) AND (header_content_type = 'text/html' OR header_content_type = '')) ORDER BY count(url) DESC",
        );
        console.log(result.fetchAllRows());
      })(),
      (async () => {
        const connection = new Connection(db);
        const result = await connection.executeIterator(
          "SELECT count(url) FROM parquet_scan('src/tests/test-fixtures/data/crawl_urls') WHERE ((http_status_code = 200 AND meta_redirect = FALSE AND primary_page = TRUE AND indexable = TRUE AND canonicalized_page = FALSE AND (paginated_page = FALSE OR (paginated_page = TRUE AND page_1 = TRUE))) AND ((css != TRUE AND js != TRUE AND is_image != TRUE AND internal = TRUE) AND (header_content_type = 'text/html' OR header_content_type = ''))) ORDER BY count(url) DESC",
        );
        console.log(result.fetchAllRows());
      })(),
    ]);
  });
});
