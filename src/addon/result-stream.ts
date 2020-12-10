import { Readable } from "stream";

import { ResultIterator } from "./result-iterator";
/**
 * This is a Readable stream that wrapps the ResultIterator. Instances of this class are returned by {@link Connection.execute | Connection.execute}.
 * @public
 */
export class ResultStream<T> extends Readable {
  /**
   * @internal
   */
  constructor(private resultIterator: ResultIterator<T>) {
    super({ objectMode: true });
  }

  /**
   * @internal
   */
  public _read(): void {
    try {
      const element = this.resultIterator.fetchRow();
      this.push(element);
      if (element === null) {
        this.close();
      }
    } catch (e) {
      this.destroy(e);
    }
  }
  /**
   * 
   * @internal
   */
  public _destroy(error: Error | null, callback: (error?: Error | null) => void): void {
    this.close();
    callback(error);
  }

  private close(): void {
    this.resultIterator.close();
    if (!this.resultIterator.isClosed) {
      this.emit("error", new Error("Close wasn't successful"));
      return;
    }
    this.emit("close");
  }
}
