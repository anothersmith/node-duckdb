/* eslint-disable */
import { read, stat, open, promises } from "fs";

import { Connection, DuckDB } from "@addon";
import { AccessMode, IFileSystem } from "@addon-types";
import {join} from "path";

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
  read: (
    fd: number,
    buffer: Buffer,
    length: number,
    callback: (error: Error | null, buffer: Buffer, bytesRead: number) => void,
  ) => {
    read(fd, buffer, 0, length, null, (error, bytesRead, filledBuffer) => {
      callback(error, filledBuffer, bytesRead);
    });
  },
  glob: (path: string, callback) => {
    glob(path, (error, matches) => {
      callback(error, matches);
    });
  },
  getFileSize: (path: string, callback: (error: Error | null, size: number) => void) => {
    stat(path, (error, stats) => {
      callback(error, stats.size);
    });
  },
  // TODO: _fileLockType
  openFile: (
    path: string,
    flags: number,
    _fileLockType: number,
    callback: (error: Error | null, fd: number) => void,
  ) => {
    open(path, flags, (error, fd) => {
      callback(error, fd);
    });
  },
};

describe("Node filesystem", () => {
  beforeAll(async () => {
    await promises.chmod("src/tests/test-fixtures/alltypes_plain_unreadable.parquet", 0o000);
  });
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
  it("handles errors - non existent file", async () => {
    const db = new DuckDB({ options: { accessMode: AccessMode.ReadWrite, useDirectIO: false } }, fileSystem);
    const connection1 = new Connection(db);
    await expect(
      connection1.executeIterator(
        "SELECT count(*) FROM parquet_scan('src/tests/test-fixtures/alltypes_plain_unreadable.parquet')",
      ),
    ).rejects.toThrow(`EACCES: permission denied, open 'src/tests/test-fixtures/alltypes_plain_unreadable.parquet'`);
    await connection1.close();
    await db.close();
  });
});
