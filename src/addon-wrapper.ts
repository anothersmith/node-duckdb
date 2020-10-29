/* eslint-disable max-classes-per-file */
import bindings from "bindings";

const { ConnectionWrapper: ConnectionWrapperBinding, ResultWrapper: ResultWrapperBinding } = bindings(
  "node-duckdb-addon",
);

declare class ConnectionWrapperClass {
  public execute(command: string, forceMaterialized?: boolean): Promise<ResultWrapperClass>;
}

declare class ResultWrapperClass {
  public fetchRow(): unknown[];
  public describe(): string[][];
  public type: ResultType;
}

export enum ResultType {
  Materialized = "Materialized",
  Streaming = "Streaming",
}

export const ConnectionWrapper: typeof ConnectionWrapperClass = ConnectionWrapperBinding;
export const ResultWrapper: typeof ResultWrapperClass = ResultWrapperBinding;
