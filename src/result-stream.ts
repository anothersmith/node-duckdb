import { Readable } from "stream";

import { ResultWrapperClass } from "./addon-wrapper";

export class ResultStream extends Readable {
  constructor(private rw: ResultWrapperClass) {
    super({ objectMode: true });
  }

  public _read(): void {
    try {
      this.push(this.rw.fetchRow());
    } catch (e) {
      this.destroy(e);
    }
  }

  // TODO: possibly implement
  // _destroy(error: Error | null, callback: (error?: Error | null) => void): void;
}
