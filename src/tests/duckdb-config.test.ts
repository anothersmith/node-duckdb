/* eslint-disable */
import { Connection, DuckDB } from "@addon"
import { AccessMode, OrderByNullType, OrderType } from "@addon-types";
import { join } from "path";

describe("DuckDB configuration", () => {
    it.skip("allows to open database in memory", () => {
        const db = new DuckDB();
        db.close();
    });
    it.skip("allows to open existing database from file", () => {
        const db = new DuckDB({path: join(__dirname, "./mydb")});
        db.close();
        // TODO: read from it
    });

    it("allows to specify access mode - read only", async () => {
        const db1 = new DuckDB({path: join(__dirname, "./mydb")});
        const connection1 = new Connection(db1);
        await connection1.executeIterator("CREATE TABLE test1 (a INTEGER);");
        await connection1.executeIterator("INSERT INTO test1 SELECT 1;");
        db1.close();
        const db2 = new DuckDB({path: join(__dirname, "./mydb"), options: {accessMode: AccessMode.ReadOnly}});
        expect(db2.accessMode).toBe(AccessMode.ReadOnly);
        const connection2 = new Connection(db2);
        await expect(connection2.executeIterator("CREATE TABLE test2 (a INTEGER);")).rejects.toMatchObject({message: `Cannot execute statement of type "CREATE" in read-only mode!`});
        db2.close();
    });

    it("allows to specify checkpoint WAL size", async () => {
        const db = new DuckDB({options: {checkPointWALSize: 1 << 30}});
        expect(db.checkPointWALSize).toBe(1 << 30);
        db.close();
    });

    it("allows to specify maximum memory", async () => {
        const db = new DuckDB({options: {maximumMemory: 5e+8}});
        expect(db.maximumMemory).toBe(5e+8);
        db.close();
    });

    // FIXME: why doesn't work?
    it.only("allows to specify whether to use temp dir", async () => {
        const db = new DuckDB({options: {useTemporaryDirectory: false, temporaryDirectory: __dirname}});
        expect(db.useTemporaryDirectory).toBe(false);
        db.close();
    });

    it("allows to specify temp dir", async () => {
        const db = new DuckDB({options: {temporaryDirectory: join(__dirname, "./tempdir")}});
        expect(db.temporaryDirectory).toBe(join(__dirname, "./tempdir"));
        db.close();
    });

    it("allows to specify collation", async () => {
        const db = new DuckDB({options: {collation: "binary"}});
        expect(db.collation).toBe("binary");
        db.close();
    });

    it("allows to specify default order type", async () => {
        const db = new DuckDB({options: {defaultOrderType: OrderType.Descending}});
        expect(db.defaultOrderType).toBe(OrderType.Descending);
        db.close();
    });

    it("allows to specify default null order type", async () => {
        const db = new DuckDB({options: {defaultNullOrder: OrderByNullType.NullsLast}});
        expect(db.defaultNullOrder).toBe(OrderByNullType.NullsLast);
        db.close();
    });

    it("allows to specify enable copy", async () => {
        const db = new DuckDB({options: {enableCopy: false}});
        expect(db.enableCopy).toBe(false);
        db.close();
    });
})
