import { ConnectionBinding } from "../addon-bindings";
import { ResultStream } from "./result-stream";
import {DuckDB} from "./duckdb";
import { ResultIterator } from "./result-iterator";

export class Connection {
  constructor(private duckdb: DuckDB) {}
  private connectionBinding = new ConnectionBinding(this.duckdb.db);
  public async execute(command: string, forceMaterialized?: boolean): Promise<ResultStream> {
    const resultIteratorBinding = await this.connectionBinding.execute(command, forceMaterialized);
    return new ResultStream(new ResultIterator(resultIteratorBinding));
  }
  public async executeIterator(command: string, forceMaterialized?: boolean): Promise<ResultIterator> {
    return new ResultIterator(await this.connectionBinding.execute(command, forceMaterialized));
  }
  public close(): void {
    return this.connectionBinding.close();
  }
  public get isClosed(): boolean {
    return this.connectionBinding.isClosed;
  }
}
