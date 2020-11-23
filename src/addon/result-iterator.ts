import { ResultType } from "@addon-types";
import { ResultIteratorClass } from "../addon-bindings";

export class ResultIterator {
    constructor(private resultInterator: ResultIteratorClass) {}
    public fetchRow<T>(): T {
        return this.resultInterator.fetchRow<T>();
    }
    public fetchAllRows<T>(): T[] {
        const allRows: T[] = [];
        for (let element = this.fetchRow<T>(); element !== null; element = this.fetchRow<T>()) {
            allRows.push(element);
          }
        return allRows;
    }
    public describe(): string[][] {
        return this.resultInterator.describe();
    }
    public close(): void {
        return this.resultInterator.close();
    }
    public get type(): ResultType {
        return this.resultInterator.type;
    }
    public get isClosed(): boolean {
        return this.resultInterator.isClosed;
    }
}
