import { ResultType } from "@addon-types";
import { ResultIteratorClass } from "../addon-bindings";
import jsonBigInt from "json-bigint";

export class ResultIterator<T> {
    constructor(private resultInterator: ResultIteratorClass<T>) {}
    public fetchRow(): T | null {
        const r = this.resultInterator.fetchRow();
        return r;
    }
    public fetchRowJson(): string | null {
        const row = this.fetchRow();
        if (row === null) {
            return null;
        }
        return jsonBigInt.stringify(row);
    }
    public fetchAllRows(): T[] {
        const allRows: T[] = [];
        for (let element = this.fetchRow(); element !== null; element = this.fetchRow()) {
            allRows.push(element);
          }
        return allRows;
    }
    public fetchAllRowsJson(): string {
        return jsonBigInt.stringify(this.fetchAllRows());
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
