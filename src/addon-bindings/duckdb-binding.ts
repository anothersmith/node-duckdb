import bindings from "bindings";

/**
 * Bindings should not be used directly, only through the addon wrappers
 */

const { DuckDB } = bindings(
  "node-duckdb-addon",
);

export declare class DuckDBClass {

}

export const DuckDBBinding: typeof DuckDBClass = DuckDB;
