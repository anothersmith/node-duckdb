const { ConnectionWrapper, ResultWrapper } = require("bindings")(
  "node-duckdb-addon"
);

declare class IConnectionWrapper {
  execute(command: string): IResultWrapper;
}

declare class IResultWrapper {
  public fetchRow(): any[];
  public describe(): string[][];
}

export default {ConnectionWrapper: <typeof IConnectionWrapper> ConnectionWrapper, ResultWrapper: <typeof IResultWrapper> ResultWrapper};
