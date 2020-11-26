import { ResultStream } from "@addon";

export function readStream<T>(rs: ResultStream<T>): Promise<T[]> {
  return new Promise((resolve, reject) => {
    const elements: T[] = [];
    rs.on("data", (el: any) => elements.push(el));
    rs.on("error", reject);
    rs.on("end", () => resolve(elements));
  });
}
