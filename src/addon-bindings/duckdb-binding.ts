import bindings from "bindings";

const { DuckDB } = bindings("node-duckdb-addon");

/**
 * Bindings should not be used directly, only through the addon wrappers
 */

export declare class DuckDBClass {
  public close(): void;
  public isClosed: boolean;
}

export const DuckDBBinding: typeof DuckDBClass = DuckDB;
