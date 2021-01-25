/* eslint-disable */
import { read, stat, open } from "fs";

import { Connection, DuckDB } from "@addon";
import { AccessMode, IFileSystem } from "@addon-types";

import glob from "glob";

const fileSystem: IFileSystem = {
  readWithLocation: (
    fd: number,
    buffer: Buffer,
    length: number,
    position: number,
    callback: (err: Error | null, buffer: Buffer) => void,
  ) => {
    read(fd, buffer, 0, length, position, (error, _bytesRead, filledBuffer) => {
      callback(error, filledBuffer);
    });
  },
  read: (fd: number, buffer: Buffer, length: number, callback: (buffer: Buffer, bytesRead: number) => void) => {
    read(fd, buffer, 0, length, null, (_err, bytesRead, filledBuffer) => {
      callback(filledBuffer, bytesRead);
    });
  },
  glob: (path: string, callback) => {
    glob(path, (_err, matches) => {
      callback(matches);
    });
  },
  getFileSize: (path: string, callback: (size: number) => void) => {
    stat(path, (_err, stats) => {
      callback(stats.size);
    });
  },
  // TODO: _fileLockType
  openFile: (path: string, flags: number, _fileLockType: number, callback: (fd: number) => void) => {
    open(path, flags, (_err, fd) => {
      callback(fd);
    });
  },
};

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
