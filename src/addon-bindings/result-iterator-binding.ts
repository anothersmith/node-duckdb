import bindings from "bindings";

import { ResultType } from "../addon-types/result-type";

/**
 * Bindings should not be used directly, only through the addon wrappers
 */

const { ResultIterator } = bindings(
  "node-duckdb-addon",
);

export declare class ResultIteratorClass {
  public fetchRow(): unknown[];
  public describe(): string[][];
  public close(): void;
  public type: ResultType;
  public isClosed: boolean;
}

export const ResultIteratorBinding: typeof ResultIteratorClass = ResultIterator;
