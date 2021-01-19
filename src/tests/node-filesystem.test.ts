import { Connection, DuckDB } from "@addon";
import { AccessMode } from "@addon-types";
import {read, openSync} from "fs";

/* eslint-disable */

describe("Node filesystem", () => {
  it("allows reading from file", async () => {
    const db = new DuckDB({ options: { accessMode: AccessMode.ReadWrite, useDirectIO: false } }, <any> ((path: any, buffer: any, length: any, position: any, b: any) => {
      console.log("In nodejs fs");
      const fd = openSync(path, "r");

      read(fd, buffer, 0, length, position, (_err, _bytesRead, filledBuffer) => {
        console.log(filledBuffer);
        b(filledBuffer) 
      })
    }));
    const connection1 = new Connection(db);
    const result = await connection1.executeIterator(
      "SELECT count(*) FROM parquet_scan('src/tests/test-fixtures/alltypes_plain.parquet')",
    );
    expect(result.fetchRow()).toMatchObject({ "count()": 8n });
  });
});
