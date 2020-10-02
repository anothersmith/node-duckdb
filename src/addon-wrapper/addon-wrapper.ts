const { ConnectionWrapper: ConnectionWrapperBinding, ResultWrapper: ResultWrapperBinding } = require("bindings")(
  "node-duckdb-addon"
);

declare class ConnectionWrapperClass {
  execute(command: string): ResultWrapperClass;
}

declare class ResultWrapperClass {
  public fetchRow(): any[];
  public describe(): string[][];
}

export const ConnectionWrapper: typeof ConnectionWrapperClass = ConnectionWrapperBinding;
export const ResultWrapper: typeof ResultWrapperClass = ResultWrapperBinding;
