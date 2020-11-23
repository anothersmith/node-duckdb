import { ConnectionBinding } from "../addon-bindings";
import { ResultStream } from "./result-stream";
import {DuckDB} from "./duckdb";
import { ResultIterator } from "./result-iterator";
import { IExecuteOptions } from "@addon-types";

export class Connection {
  constructor(private duckdb: DuckDB) {}
  private connectionBinding = new ConnectionBinding(this.duckdb.db);
  public async execute<T>(command: string, options?: IExecuteOptions): Promise<ResultStream<T>> {
    const resultIteratorBinding = await this.connectionBinding.execute<T>(command, options);
    return new ResultStream(new ResultIterator(resultIteratorBinding));
  }
  public async executeIterator<T>(command: string, options?: IExecuteOptions): Promise<ResultIterator<T>> {
    return new ResultIterator(await this.connectionBinding.execute<T>(command, options));
  }
  public close(): void {
    return this.connectionBinding.close();
  }
  public get isClosed(): boolean {
    return this.connectionBinding.isClosed;
  }
}
