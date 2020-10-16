/* eslint-disable max-classes-per-file */

import {Worker} from "worker_threads";

import {join} from "path";


export class ResultWrapper {
  // private resultWrapperId = 1;
  constructor(private worker: Worker) {}

  public async fetchRow(): Promise<unknown[]> {
    return new Promise((resolve) => {
      this.worker.on("fetched", resolve)
      this.worker.postMessage({command: "fetchRow"})
    })
  }
}

export class ConnectionWrapper {
  private worker = new Worker(join(__dirname, "./worker.js"))

  public execute(query: string): Promise<ResultWrapper> {
    return new Promise((resolve) => {
      this.worker.on("message", message => {
        console.log("--------gdfgdfds")
        if (message.event === "executed") {
          console.log("--------lllljksdas")
          resolve(new ResultWrapper(this.worker))

        }
      } );
      this.worker.postMessage({command: "execute", query});
    })
  }
}
