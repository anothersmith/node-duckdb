import { AccessMode, IDuckDBConfig, OrderByNullType, OrderType } from "@addon-types";

// the reason for this kind of import is ncc: https://github.com/vercel/ncc/pull/93
// eslint-disable-next-line @typescript-eslint/no-var-requires
const { DuckDB } = require("bindings")("node-duckdb-addon");

/**
 * Bindings should not be used directly, only through the addon wrappers
 */

export declare class DuckDBClass {
  constructor(config: IDuckDBConfig);
  public close(): void;
  public isClosed: boolean;
  public accessMode: AccessMode;
  public checkPointWALSize: number;
  public maximumMemory: number;
  public useTemporaryDirectory: boolean;
  public temporaryDirectory: string;
  public collation: string;
  public defaultOrderType: OrderType;
  public defaultNullOrder: OrderByNullType;
  public enableCopy: boolean;
}

export const DuckDBBinding: typeof DuckDBClass = DuckDB;
