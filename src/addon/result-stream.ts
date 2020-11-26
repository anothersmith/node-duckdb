import { Readable } from "stream";

import { ResultIterator } from "./result-iterator";

export class ResultStream<T> extends Readable {
  constructor(private resultIterator: ResultIterator<T>, serializedJson?: boolean) {
    super({ objectMode: !serializedJson });
    if (serializedJson) {
      this.setEncoding("utf-8")
    }
  }

  public _read(): void {
    try {
      const element = this.readableObjectMode ? this.resultIterator.fetchRow() : this.resultIterator.fetchRowJson();
      this.push(element);
      if (element === null) {
        this.close();
      }
    } catch (e) {
      this.destroy(e);
    }
  }

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
