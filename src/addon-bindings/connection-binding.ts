import { IExecuteOptions } from "@addon-types";


/**
 * Bindings should not be used directly, only through the addon wrappers
 */

import { DuckDBBinding } from "./duckdb-binding";
import { ResultIteratorBinding } from "./result-iterator-binding";

// lambda doesn't work with npm module bindings
// eslint-disable-next-line node/no-unpublished-require, @typescript-eslint/no-var-requires
const { Connection } = require("../../build/Release/node-duckdb-addon.node");

export declare class ConnectionClass {
  constructor(db: InstanceType<typeof DuckDBBinding>);
  public execute(command: string, options?: IExecuteOptions): Promise<InstanceType<typeof ResultIteratorBinding>>;
  public close(): void;
  public isClosed: boolean;
}

export const ConnectionBinding: typeof ConnectionClass = Connection;
