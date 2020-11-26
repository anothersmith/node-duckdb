import { Connection, DuckDB } from "@addon";

import { serializedSetArray, entireSerializedResultSet } from "./test-fixtures/json-serialization";
import { readStream } from "./utils";

const query = "SELECT * FROM parquet_scan('src/tests/test-fixtures/alltypes_plain.parquet')";

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
    expect(row).toEqual(serializedSetArray[0]);
  });

  it("is supported by iterator - fetchAllRowsJson", async () => {
    const result = await connection.executeIterator(query);
    const resultSet = result.fetchAllRowsJson();
    expect(resultSet).toEqual(entireSerializedResultSet);
  });

  it("is supported by stream", async () => {
    const rs = await connection.execute<string>(query, { serializedJson: true });
    const allRows = await readStream(rs);
    expect(allRows).toEqual(serializedSetArray);
  });
});