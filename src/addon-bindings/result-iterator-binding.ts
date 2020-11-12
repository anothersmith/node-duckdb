import { ResultType } from "@addon-types";

// the reason for this kind of import is ncc: https://github.com/vercel/ncc/pull/93
// eslint-disable-next-line @typescript-eslint/no-var-requires
const { ResultIterator } = require("bindings")("node-duckdb-addon");

/**
 * Bindings should not be used directly, only through the addon wrappers
 */

export declare class ResultIteratorClass {
  public fetchRow(): unknown | unknown[];
  public describe(): string[][];
  public close(): void;
  public type: ResultType;
  public isClosed: boolean;
}

export const ResultIteratorBinding: typeof ResultIteratorClass = ResultIterator;
