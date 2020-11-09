import { ResultType } from "@addon-types";
import bindings from "bindings";

const { ResultIterator } = bindings("node-duckdb-addon");

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
