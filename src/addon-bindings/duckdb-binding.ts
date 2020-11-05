import { AccessMode, IDuckDBConfig, OrderByNullType, OrderType } from "@addon-types";
import bindings from "bindings";

const { DuckDB } = bindings("node-duckdb-addon");

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
