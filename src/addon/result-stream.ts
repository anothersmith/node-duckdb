import { Readable } from "stream";

import { ResultWrapperClass } from "./bindings";

export class ResultStream extends Readable {
  constructor(private rw: ResultWrapperClass) {
    super({ objectMode: true });
  }

  public _read(): void {
    try {
      const element = this.rw.fetchRow();
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
    this.rw.close();
    if (!this.rw.isClosed) {
      this.emit("error", new Error("Close wasn't successful"));
      return;
    }
    this.emit("close");
  }
}
