import { IExecuteOptions } from "@addon-types";

// the reason for this kind of import is ncc: https://github.com/vercel/ncc/pull/93
// eslint-disable-next-line import/order
const { Connection } = require("bindings")("node-duckdb-addon");

/**
 * Bindings should not be used directly, only through the addon wrappers
 */

import { DuckDBBinding } from "./duckdb-binding";
import { ResultIteratorBinding } from "./result-iterator-binding";

export declare class ConnectionClass {
  constructor(db: InstanceType<typeof DuckDBBinding>);
  public execute(command: string, options?: IExecuteOptions): Promise<InstanceType<typeof ResultIteratorBinding>>;
  public close(): void;
  public isClosed: boolean;
}

export const ConnectionBinding: typeof ConnectionClass = Connection;
