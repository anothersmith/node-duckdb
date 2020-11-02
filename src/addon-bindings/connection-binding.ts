import bindings from "bindings";

/**
 * Bindings should not be used directly, only through the addon wrappers
 */

const { Connection } = bindings(
  "node-duckdb-addon",
);

import {DuckDBBinding} from "./duckdb-binding";

import {ResultIteratorBinding} from "./result-iterator-binding";

export declare class ConnectionWrapperClass {
  constructor(db: InstanceType<typeof DuckDBBinding>);
  public execute(command: string, forceMaterialized?: boolean): Promise<InstanceType<typeof ResultIteratorBinding>>;
}

export const ConnectionBinding: typeof ConnectionWrapperClass = Connection;
