import { ResultType } from "@addon-types";

// lambda doesn't work with npm module bindings
// eslint-disable-next-line node/no-unpublished-require, @typescript-eslint/no-var-requires
const { ResultIterator } = require("../../build/Release/node-duckdb-addon.node");
/**
 * Bindings should not be used directly, only through the addon wrappers
 */

export declare class ResultIteratorClass<T> {
  public fetchRow(): T;
  public describe(): Array<[string, string]>;
  public close(): void;
  public type: ResultType;
  public isClosed: boolean;
}

export const ResultIteratorBinding: typeof ResultIteratorClass = ResultIterator;
