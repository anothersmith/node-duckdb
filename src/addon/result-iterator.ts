import { ResultType } from "@addon-types";
import { ResultIteratorClass } from "../addon-bindings";

export class ResultIterator {
    constructor(private resultInterator: ResultIteratorClass) {}
    public fetchRow(): unknown[] {
        return this.resultInterator.fetchRow();
    }
    public fetchAllRows(): unknown[][] {
        const allRows: unknown[][] = [];
        for (let element = this.fetchRow(); element !== null; element = this.fetchRow()) {
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
