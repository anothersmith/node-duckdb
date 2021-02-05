/* eslint-disable */
import { read, open, promises } from "fs";

import { Connection, DuckDB } from "@addon";
import { AccessMode, IFileSystem, RowResultFormat } from "@addon-types";
import { join } from "path";

import glob from "glob";
import { fileSystem } from "./node-filesystem";

const { stat, unlink } = promises;

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
  it("allows reading from file - multiple at once", async () => {
    const db = new DuckDB({ options: { accessMode: AccessMode.ReadWrite, useDirectIO: false } }, fileSystem);
    await Promise.all([
      (async () => {
        const connection1 = new Connection(db);
        const result = await connection1.executeIterator(
          "SELECT count(*) FROM parquet_scan('src/tests/test-fixtures/alltypes_plain.parquet')",
        );
        expect(result.fetchRow()).toMatchObject({ "count()": 8n });
        await connection1.close();
      })(),
      (async () => {
        const connection1 = new Connection(db);
        const result = await connection1.executeIterator(
          "SELECT count(*) FROM parquet_scan('src/tests/test-fixtures/alltypes_plain.parquet')",
        );
        expect(result.fetchRow()).toMatchObject({ "count()": 8n });
        await connection1.close();
      })(),
      (async () => {
        const connection1 = new Connection(db);
        const result = await connection1.executeIterator(
          "SELECT count(*) FROM parquet_scan('src/tests/test-fixtures/alltypes_plain.parquet')",
        );
        expect(result.fetchRow()).toMatchObject({ "count()": 8n });
        await connection1.close();
      })()
    ])
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
  it.skip("allows writing to database", async () => {
    const dbPath = join(__dirname, "./mydb");
    const db1 = new DuckDB({ path: dbPath, options: { accessMode: AccessMode.ReadWrite, useDirectIO: false } }, fileSystem);
    const connection1 = new Connection(db1);
    await connection1.executeIterator("CREATE TABLE test2 (a INTEGER);");
    await connection1.executeIterator("INSERT INTO test2 SELECT 1;");
    db1.close();
    const fd = await stat(dbPath);
    expect(fd.isFile()).toBe(true);
    const db2 = new DuckDB({ path: dbPath, options: { accessMode: AccessMode.ReadOnly } });
    expect(db2.accessMode).toBe(AccessMode.ReadOnly);
    const connection2 = new Connection(db2);
    const iterator = await connection2.executeIterator("SELECT * FROM test2;", {
      rowResultFormat: RowResultFormat.Array,
    });
    expect(iterator.fetchRow()).toEqual([1]);
    db2.close();
    await unlink(dbPath);
  });
});
