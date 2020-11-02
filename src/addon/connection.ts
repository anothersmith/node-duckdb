import { ConnectionWrapper as ConnectionWrapperBinding, ResultWrapperClass, DuckDBClass } from "./bindings";
import { ResultStream } from "./result-stream";

export class ConnectionWrapper {
  constructor(private db: DuckDBClass) {}
  private connectionWrapperBinding = new ConnectionWrapperBinding(this.db);
  public async execute(command: string, forceMaterialized?: boolean): Promise<ResultStream> {
    const rw = await this.connectionWrapperBinding.execute(command, forceMaterialized);
    return new ResultStream(rw);
  }

  public async executeIterator(command: string, forceMaterialized?: boolean): Promise<ResultWrapperClass> {
    return this.connectionWrapperBinding.execute(command, forceMaterialized);
  }
}
