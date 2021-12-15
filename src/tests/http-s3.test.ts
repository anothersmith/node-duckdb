import { Connection, DuckDB } from "@addon";
import { IExecuteOptions, RowResultFormat } from "@addon-types";

const executeOptions: IExecuteOptions = { rowResultFormat: RowResultFormat.Array };

jest.setTimeout(60 * 1000 * 2);
describe("Http/s3 interface", () => {
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

  it("allows reading from https", async () => {
    const result = await connection.executeIterator(
      "SELECT * FROM parquet_scan('https://github.com/deepcrawl/node-duckdb/raw/master/src/tests/test-fixtures/alltypes_plain.parquet')",
      executeOptions,
    );
    expect(result.fetchRow()).toMatchObject([
      4,
      true,
      0,
      0,
      0,
      0n,
      0,
      0,
      Buffer.from("03/01/09"),
      Buffer.from("0"),
      1235865600000,
    ]);
  });

  /**
   * - test needs AWS creds which have the permission to read from "amazon-reviews-pds" bucket
   * - when running in github actions "dforsber-duckdb-test" (Dev environment) user is used
   * - when running locally the "run-tests-locally" script loads credentials from your aws setup
   */
  it("allows reading from s3", async () => {
    await connection.executeIterator(`SET s3_region='us-east-1'`);
    await connection.executeIterator(`SET s3_access_key_id='${process.env.AWS_ACCESS_KEY_ID}'`);
    await connection.executeIterator(`SET s3_secret_access_key='${process.env.AWS_SECRET_ACCESS_KEY}'`);
    if (process.env.AWS_SESSION_TOKEN) {
      await connection.executeIterator(`SET s3_session_token='${process.env.AWS_SESSION_TOKEN}'`);
    }
    const result = await connection.executeIterator(
      "SELECT * FROM parquet_scan('s3://amazon-reviews-pds/parquet/product_category=Books/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet') LIMIT 1",
      executeOptions,
    );
    expect(result.fetchRow()).toMatchObject([
      "US",
      "15444933",
      "R1WWG70WK9VUCH",
      "1848192576",
      "835940987",
      "Standing Qigong for Health and Martial Arts - Zhan Zhuang",
      5,
      9,
      10,
      "N",
      "Y",
      "Informative AND interesting!",
      "After attending a few Qigong classes, I wanted to have a book to read and re-read the instructions so I could practice at home.  I also wanted to gain more of an understanding of the purpose and benefit of the movements in order to practice them with a more focused purpose.<br /><br />The book exceeded my expectations.  The explanations are very clear and are paired with photos showing the correct form.  The book itself is more than just the Qigong, it's a very interesting read.  I read the whole book in two days and will read it again. I rarely read books twice!  The book has provided the information and additional instruction that I was looking for. I even use the breathing exercise to de-stress in traffic and fall asleep at night.  It really works!  I bought the book for my sister also and she's started practicing Standing Qigong and loves it.",
      "2015-05-02",
      2015,
    ]);
  });
});
