import { AccessMode, IDuckDBConfig, OrderByNullType, OrderType } from "@addon-types";

// lambda doesn't work with npm module bindings
// eslint-disable-next-line node/no-unpublished-require, @typescript-eslint/no-var-requires
const { DuckDB } = require("../../build/Release/node-duckdb-addon.node");

/**
 * Bindings should not be used directly, only through the addon wrappers
 */

export declare class DuckDBClass {
  constructor(config: IDuckDBConfig, fileSystemCallback?: () => any);
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
