/* eslint-disable */
import { read, openSync } from "fs";

import { Connection, DuckDB } from "@addon";
import { AccessMode, IFileSystem } from "@addon-types";

import glob from "glob";

let pos = 0;

const fileSystem: IFileSystem = {
  readWithLocation: (path: string,
    buffer: Buffer,
    length: number,
    position: number,
    callback: (buffer: Buffer) => void) => {
      console.log("In nodejs fs");
      console.log(position);
      pos = position;
      const fd = openSync(path, "r");

      read(fd, buffer, 0, length, position, (_err, _bytesRead, filledBuffer) => {
        callback(filledBuffer);
      });
    },
    read: (path: string,
      buffer: Buffer,
      length: number,
      callback: (buffer: Buffer, bytesRead: number) => void) => {
        console.log("In nodejs fs 111");
        const fd = openSync(path, "r");
  
        read(fd, buffer, 0, length, pos, (_err, bytesRead, filledBuffer) => {
          callback(filledBuffer, bytesRead);
        });
      },
    glob: (path: string, callback) => {
      glob(path, (_err, matches) => {
        console.log(matches);
        callback(matches)
      });
    }
}

describe("Node filesystem", () => {
  it("allows reading from file", async () => {
    const db = new DuckDB({ options: { accessMode: AccessMode.ReadWrite, useDirectIO: false } }, fileSystem);
    const connection1 = new Connection(db);
    const result = await connection1.executeIterator(
      "SELECT count(*) FROM parquet_scan('src/tests/test-fixtures/alltypes_plain.parquet')",

    );
    expect(result.fetchRow()).toMatchObject({ "count()": 8n });
    await connection1.close();
    await db.close();
  });
});
