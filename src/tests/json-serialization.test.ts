import { Connection, DuckDB } from "@addon";

import { readStream } from "./utils";

const wholeSerializedResultSet = `[{"id":4,"bool_col":true,"tinyint_col":0,"smallint_col":0,"int_col":0,"bigint_col":0,"float_col":0,"double_col":0,"date_string_col":"03/01/09","string_col":"0","timestamp_col":1235865600000},{"id":5,"bool_col":false,"tinyint_col":1,"smallint_col":1,"int_col":1,"bigint_col":10,"float_col":1.100000023841858,"double_col":10.1,"date_string_col":"03/01/09","string_col":"1","timestamp_col":1235865660000},{"id":6,"bool_col":true,"tinyint_col":0,"smallint_col":0,"int_col":0,"bigint_col":0,"float_col":0,"double_col":0,"date_string_col":"04/01/09","string_col":"0","timestamp_col":1238544000000},{"id":7,"bool_col":false,"tinyint_col":1,"smallint_col":1,"int_col":1,"bigint_col":10,"float_col":1.100000023841858,"double_col":10.1,"date_string_col":"04/01/09","string_col":"1","timestamp_col":1238544060000},{"id":2,"bool_col":true,"tinyint_col":0,"smallint_col":0,"int_col":0,"bigint_col":0,"float_col":0,"double_col":0,"date_string_col":"02/01/09","string_col":"0","timestamp_col":1233446400000},{"id":3,"bool_col":false,"tinyint_col":1,"smallint_col":1,"int_col":1,"bigint_col":10,"float_col":1.100000023841858,"double_col":10.1,"date_string_col":"02/01/09","string_col":"1","timestamp_col":1233446460000},{"id":0,"bool_col":true,"tinyint_col":0,"smallint_col":0,"int_col":0,"bigint_col":0,"float_col":0,"double_col":0,"date_string_col":"01/01/09","string_col":"0","timestamp_col":1230768000000},{"id":1,"bool_col":false,"tinyint_col":1,"smallint_col":1,"int_col":1,"bigint_col":10,"float_col":1.100000023841858,"double_col":10.1,"date_string_col":"01/01/09","string_col":"1","timestamp_col":1230768060000}]`

const separateExpectedJsonRows =     [
    '{"id":4,"bool_col":true,"tinyint_col":0,"smallint_col":0,"int_col":0,"bigint_col":0,"float_col":0,"double_col":0,"date_string_col":"03/01/09","string_col":"0","timestamp_col":1235865600000}',
    '{"id":5,"bool_col":false,"tinyint_col":1,"smallint_col":1,"int_col":1,"bigint_col":10,"float_col":1.100000023841858,"double_col":10.1,"date_string_col":"03/01/09","string_col":"1","timestamp_col":1235865660000}',
    '{"id":6,"bool_col":true,"tinyint_col":0,"smallint_col":0,"int_col":0,"bigint_col":0,"float_col":0,"double_col":0,"date_string_col":"04/01/09","string_col":"0","timestamp_col":1238544000000}',
    '{"id":7,"bool_col":false,"tinyint_col":1,"smallint_col":1,"int_col":1,"bigint_col":10,"float_col":1.100000023841858,"double_col":10.1,"date_string_col":"04/01/09","string_col":"1","timestamp_col":1238544060000}',
    '{"id":2,"bool_col":true,"tinyint_col":0,"smallint_col":0,"int_col":0,"bigint_col":0,"float_col":0,"double_col":0,"date_string_col":"02/01/09","string_col":"0","timestamp_col":1233446400000}',
    '{"id":3,"bool_col":false,"tinyint_col":1,"smallint_col":1,"int_col":1,"bigint_col":10,"float_col":1.100000023841858,"double_col":10.1,"date_string_col":"02/01/09","string_col":"1","timestamp_col":1233446460000}',
    '{"id":0,"bool_col":true,"tinyint_col":0,"smallint_col":0,"int_col":0,"bigint_col":0,"float_col":0,"double_col":0,"date_string_col":"01/01/09","string_col":"0","timestamp_col":1230768000000}',
    '{"id":1,"bool_col":false,"tinyint_col":1,"smallint_col":1,"int_col":1,"bigint_col":10,"float_col":1.100000023841858,"double_col":10.1,"date_string_col":"01/01/09","string_col":"1","timestamp_col":1230768060000}'
  ];
const query = "SELECT * FROM parquet_scan('src/tests/test-fixtures/alltypes_plain.parquet')"

describe("JSON serialization", () => {
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

  it("is supported by iterator - fetchRowJson", async () => {
    const result = await connection.executeIterator(query);
    const row = result.fetchRowJson();
    expect(row).toEqual(separateExpectedJsonRows[0]);
  });

  it("is supported by iterator - fetchAllRowsJson", async () => {
    const result = await connection.executeIterator(query);
    const resultSet = result.fetchAllRowsJson();
    expect(resultSet).toEqual(wholeSerializedResultSet)
  });

  it("is supported by stream", async () => {
    const rs = await connection.execute<string>(
      query,
      {jsonMode: true}
    );
    const allRows = await readStream(rs);
    expect(allRows).toEqual(separateExpectedJsonRows);
  });
});
