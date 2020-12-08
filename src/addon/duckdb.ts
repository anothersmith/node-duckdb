import { IDuckDBConfig } from "@addon-types";
import { DuckDBBinding, DuckDBClass } from "../addon-bindings";
import { promises as fs } from "fs";
import { join } from "path";

export class DuckDB {
    public static async getBindingsVersion() {
        return JSON.parse(await fs.readFile(join(__dirname, "../../package.json"), {encoding: "utf-8"})).version;
    }
    private duckdb: DuckDBClass;
    constructor(config: IDuckDBConfig = {}) {
        this.duckdb = new DuckDBBinding(config);
    }
    public close(): void {
        return this.duckdb.close();
    }
    public get db() {
        return this.duckdb;
    }
    public get isClosed(): boolean {
        return this.duckdb.isClosed;
    }
    public get accessMode() {
        return this.duckdb.accessMode;
    }
    public get checkPointWALSize() {
        return this.duckdb.checkPointWALSize;
    }
    public get maximumMemory() {
        return this.duckdb.maximumMemory;
    }
    public get useTemporaryDirectory() {
        return this.duckdb.useTemporaryDirectory;
    }
    public get temporaryDirectory() {
        return this.duckdb.temporaryDirectory;
    }
    public get collation() {
        return this.duckdb.collation;
    }
    public get defaultOrderType() {
        return this.duckdb.defaultOrderType;
    }
    public get defaultNullOrder() {
        return this.duckdb.defaultNullOrder;
    }
    public get enableCopy() {
        return this.duckdb.enableCopy;
    }
}
