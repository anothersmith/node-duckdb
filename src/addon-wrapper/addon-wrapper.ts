/* eslint-disable max-classes-per-file */
import bindings from "bindings";

const { ConnectionWrapper: ConnectionWrapperBinding, ResultWrapper: ResultWrapperBinding } = bindings(
  "node-duckdb-addon",
);

export class ConnectionWrapper {
  private connectionWrapperBinding = new ConnectionWrapperBinding();
  public execute(command: string): Promise<ResultWrapperClass> {
      return new Promise((resolve) => {
        console.log("--------herexxx")
        this.connectionWrapperBinding.execute(command, resolve);
      })
  }
}

declare class ResultWrapperClass {
  public fetchRow(): unknown[];
  public describe(): string[][];
}

export const ResultWrapper: typeof ResultWrapperClass = ResultWrapperBinding;
