import { DuckDBBinding } from "../addon-bindings";

export class DuckDB {
    private duckdb = new DuckDBBinding();

    public get db() {
        return this.duckdb;
    }
}
