/* eslint-disable */
import { Connection, DuckDB } from "@addon"
import { AccessMode, OrderByNullType, OrderType } from "@addon-types";
import { join } from "path";
import {unlink, stat} from "fs";
import {promisify} from "util";
const unlinkAsync = promisify(unlink);
const statAsync = promisify(stat);

const dbPath = join(__dirname, "./mydb");
describe("DuckDB configuration", () => {
    it("allows to create database on disk", async () => {
        const db = new DuckDB({path: dbPath});
        db.close();
        const fd = await statAsync(dbPath);
        expect(fd.isFile()).toBe(true);
        await unlinkAsync(dbPath)
    });

    it("allows to specify access mode - read only write operation rejects", async () => {
        const db1 = new DuckDB({path: dbPath});
        db1.close();
        const fd = await statAsync(dbPath);
        expect(fd.isFile()).toBe(true);
        const db2 = new DuckDB({path: dbPath, options: {accessMode: AccessMode.ReadOnly}});
        expect(db2.accessMode).toBe(AccessMode.ReadOnly);
        const connection2 = new Connection(db2);
        await expect(connection2.executeIterator("CREATE TABLE test2 (a INTEGER);")).rejects.toMatchObject({message: `Cannot execute statement of type "CREATE" in read-only mode!`});
        db2.close();
    });

    it("allows to specify access mode - read only read operation succeedes", async () => {
        const db1 = new DuckDB({path: dbPath});
        const connection1 = new Connection(db1);
        await connection1.executeIterator("CREATE TABLE test2 (a INTEGER);")
        await connection1.executeIterator("INSERT INTO test2 SELECT 1;")
        db1.close();
        const fd = await statAsync(dbPath);
        expect(fd.isFile()).toBe(true);
        const db2 = new DuckDB({path: dbPath, options: {accessMode: AccessMode.ReadOnly}});
        expect(db2.accessMode).toBe(AccessMode.ReadOnly);
        const connection2 = new Connection(db2);
        const iterator = await connection2.executeIterator("SELECT * FROM test2;");
        expect(iterator.fetchRow()).toEqual([1]);
        db2.close();
        await unlinkAsync(dbPath)
    });

    it("does not allow to specify invalid access mode", async () => {
        expect(() => new DuckDB(<any>{options: {accessMode: 10}})).toThrow("Invalid accessMode: must be of type AccessMode enum");
    });

    it("does not allow to to specify invalid options object", async () => {
        expect(() => new DuckDB(<any>{options: 1})).toThrow("Invalid options: must be an object");
    });

    it("allows to specify checkpoint WAL size", async () => {
        const db = new DuckDB({options: {checkPointWALSize: 1 << 30}});
        expect(db.checkPointWALSize).toBe(1 << 30);
        db.close();
    });

    it("does not allow to specify invalid checkpoint WAL size", async () => {
        expect(() => new DuckDB(<any>{options: {checkPointWALSize: "invalid"}})).toThrow("Invalid checkPointWALSize: must be a number");
    });

    it("allows to specify maximum memory", async () => {
        const db = new DuckDB({options: {maximumMemory: 5e+8}});
        expect(db.maximumMemory).toBe(5e+8);
        db.close();
    });

    it("does not allow to specify invalid maximum memory", async () => {
        expect(() => new DuckDB(<any>{options: {maximumMemory: "invalid"}})).toThrow("Invalid maximumMemory: must be a number");
    });

    // Note: looks like a duckdb bug: sets useTemporaryDirectory to true (although at the same time removes temporaryDirectory value)
    it("allows to specify whether to use temp dir", async () => {
        const db = new DuckDB({options: {useTemporaryDirectory: false, temporaryDirectory: __dirname}});
        expect(db.temporaryDirectory).toBeFalsy();
        db.close();
    });

    it("does not allow to specify invalid useTemporaryDirectory", async () => {
        expect(() => new DuckDB(<any>{options: {useTemporaryDirectory: "invalid"}})).toThrow("Invalid useTemporaryDirectory: must be a boolean");
    });

    it("allows to specify temp dir", async () => {
        const db = new DuckDB({options: {temporaryDirectory: join(__dirname, "./tempdir")}});
        expect(db.temporaryDirectory).toBe(join(__dirname, "./tempdir"));
        db.close();
    });

    it("does not allow to specify invalid temp dir", async () => {
        expect(() => new DuckDB(<any>{options: {temporaryDirectory: true}})).toThrow("Invalid temporaryDirectory: must be a string");
    });

    it("allows to specify collation", async () => {
        const db = new DuckDB({options: {collation: "binary"}});
        expect(db.collation).toBe("binary");
        db.close();
    });

    it("does not allow to specify invalid collation", async () => {
        expect(() => new DuckDB(<any>{options: {collation: true}})).toThrow("Invalid collation: must be a string");
    });

    it("allows to specify default order type", async () => {
        const db = new DuckDB({options: {defaultOrderType: OrderType.Descending}});
        expect(db.defaultOrderType).toBe(OrderType.Descending);
        db.close();
    });

    it("does not allow to specify invalid default order type", async () => {
        expect(() => new DuckDB(<any>{options: {defaultOrderType: 10}})).toThrow("Invalid defaultOrderType: must be of type OrderType enum");
    });

    it("allows to specify default null order type", async () => {
        const db = new DuckDB({options: {defaultNullOrder: OrderByNullType.NullsLast}});
        expect(db.defaultNullOrder).toBe(OrderByNullType.NullsLast);
        db.close();
    });

    it("does not allow to specify invalid default null order type", async () => {
        expect(() => new DuckDB(<any>{options: {defaultNullOrder: 10}})).toThrow("Invalid defaultNullOrder: must be of type OrderByNullType enum");
    });

    it("allows to specify enable copy", async () => {
        const db = new DuckDB({options: {enableCopy: false}});
        expect(db.enableCopy).toBe(false);
        db.close();
    });

    it("does not allow to specify invalid enableCopy", async () => {
        expect(() => new DuckDB(<any>{options: {enableCopy: 10}})).toThrow("Invalid enableCopy: must be a boolean");
    });
})
