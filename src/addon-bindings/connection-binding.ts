import { IExecuteOptions } from "@addon-types";

/**
 * Bindings should not be used directly, only through the addon wrappers
 */

import { DuckDBBinding } from "./duckdb-binding";
import { ResultIteratorClass } from "./result-iterator-binding";

// lambda doesn't work with npm module bindings
// eslint-disable-next-line node/no-unpublished-require, @typescript-eslint/no-var-requires
const { Connection } = require("../../build/Release/node-duckdb-addon.node");

export declare class ConnectionClass {
  constructor(db: InstanceType<typeof DuckDBBinding>);
  public execute<T>(
    command: string,
    callback: (error: string | null, resultIterator: ResultIteratorClass<T>) => void,
    options?: IExecuteOptions,
  ): void;
  public close(): void;
  public isClosed: boolean;
}

export const ConnectionBinding: typeof ConnectionClass = Connection;
