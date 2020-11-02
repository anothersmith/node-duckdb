/* eslint-disable max-classes-per-file */
import bindings from "bindings";

const { DuckDB: DuckDBBinding, ConnectionWrapper: ConnectionWrapperBinding, ResultWrapper: ResultWrapperBinding } = bindings(
  "node-duckdb-addon",
);


export declare class DuckDBClass {

}

declare class ConnectionWrapperClass {
  constructor(db: DuckDBClass);
  public execute(command: string, forceMaterialized?: boolean): Promise<ResultWrapperClass>;
}

export declare class ResultWrapperClass {
  public fetchRow(): unknown[];
  public describe(): string[][];
  public close(): void;
  public type: ResultType;
  public isClosed: boolean;
}

export enum ResultType {
  Materialized = "Materialized",
  Streaming = "Streaming",
}
console.log(DuckDBBinding);
export const DuckDB: typeof DuckDBClass = DuckDBBinding;
export const ConnectionWrapper: typeof ConnectionWrapperClass = ConnectionWrapperBinding;
export const ResultWrapper: typeof ResultWrapperClass = ResultWrapperBinding;
