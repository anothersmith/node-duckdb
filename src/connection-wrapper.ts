import { ConnectionWrapper as ConnectionWrapperBinding, ResultWrapperClass } from "./addon-wrapper";
import { ResultStream } from "./result-stream";

export class ConnectionWrapper {
  private connectionWrapperBinding = new ConnectionWrapperBinding();
  public async execute(command: string, forceMaterialized?: boolean): Promise<ResultStream> {
    const rw = await this.connectionWrapperBinding.execute(command, forceMaterialized);
    return new ResultStream(rw);
  }

  public async executeIterator(command: string, forceMaterialized?: boolean): Promise<ResultWrapperClass> {
    return this.connectionWrapperBinding.execute(command, forceMaterialized);
  }
}
