import { Readable } from "stream";

import { ResultIterator } from "./result-iterator";

export function getResultStream<T>(iterator: ResultIterator<T>): Readable {
  return Readable.from(<any>iterator, {
    destroy() {
      iterator.close();
    },
  });
}
