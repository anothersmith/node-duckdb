/* eslint-disable max-classes-per-file */
import bindings from "bindings";

const { ConnectionWrapper: ConnectionWrapperBinding, ResultWrapper: ResultWrapperBinding, startWork } = bindings(
  "node-duckdb-addon",
);

declare class ConnectionWrapperClass {
  public execute(command: string): ResultWrapperClass;
}

declare class ResultWrapperClass {
  public fetchRow(): unknown[];
  public describe(): string[][];
}

export const ConnectionWrapper: typeof ConnectionWrapperClass = ConnectionWrapperBinding;
export const ResultWrapper: typeof ResultWrapperClass = ResultWrapperBinding;
export const a = startWork;
