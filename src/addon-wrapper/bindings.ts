/* eslint-disable max-classes-per-file */
import bindings from "bindings";

const { ConnectionWrapper: ConnectionWrapperBinding, ResultWrapper: ResultWrapperBinding } = bindings(
  "node-duckdb-addon",
);

export declare class ConnectionWrapperClass {
  public execute(command: string): ResultWrapperClass;
}

export declare class ResultWrapperClass {
  public fetchRow(): unknown[];
  public describe(): string[][];
}

export const ConnectionWrapper: typeof ConnectionWrapperClass = ConnectionWrapperBinding;
export const ResultWrapper: typeof ResultWrapperClass = ResultWrapperBinding;
