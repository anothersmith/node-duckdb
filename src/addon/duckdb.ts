import { DuckDBBinding } from "../addon-bindings";

export class DuckDB {
    private duckdb = new DuckDBBinding();
    public close(): void {
        return this.duckdb.close();
    }
    public get isClosed(): boolean {
        return this.duckdb.isClosed;
    }
    public get db() {
        return this.duckdb;
    }
}
