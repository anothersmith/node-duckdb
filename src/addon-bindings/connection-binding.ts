import bindings from "bindings";

const { Connection } = bindings("node-duckdb-addon");

/**
 * Bindings should not be used directly, only through the addon wrappers
 */

import { DuckDBBinding } from "./duckdb-binding";
import { ResultIteratorBinding } from "./result-iterator-binding";

export declare class ConnectionClass {
  constructor(db: InstanceType<typeof DuckDBBinding>);
  public execute(command: string, forceMaterialized?: boolean): Promise<InstanceType<typeof ResultIteratorBinding>>;
}

export const ConnectionBinding: typeof ConnectionClass = Connection;
