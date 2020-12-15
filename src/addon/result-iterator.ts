import { ResultIteratorClass } from "@addon-bindings";
import { ResultType } from "@addon-types";

/**
 * ResultIterator represents the result set of a DuckDB query. Instances of this class are returned by the {@link Connection.executeIterator | Connection.executeIterator}.
 *
 * @public
 */
export class ResultIterator<T> {
  /**
   *
   * @internal
   */
  constructor(private resultInterator: ResultIteratorClass<T>) {}
  /**
   * Fetch the next row
   *
   * @remarks
   * First call returns the first row, when no more rows left `null` is returned.
   */
  public fetchRow(): T {
    return this.resultInterator.fetchRow();
  }
  /**
   * Fetch all rows
   *
   * @remarks
   * Note, this may produce a `heap out of bounds` error in case when there is too much data. Either use the {@link ResultIterator.fetchRow | fetchRow} or the  {@link Connection.execute | Connection.execute} method when there is a lot of data.
   */
  public fetchAllRows(): T[] {
    return <T[]>[...this];
  }
  /**
   * Describe the result set schema.
   */
  public describe(): string[][] {
    return this.resultInterator.describe();
  }
  /**
   * Close the ResultIterator
   * @remarks
   * {@link Connection.close | Connection.close} automatically closes all associated ResultIterators.
   */
  public close(): void {
    return this.resultInterator.close();
  }
  /**
   * Get the {@link ResultType | ResultType} of the ResultIterator. This is specified by the {@link IExecuteOptions.forceMaterialized | options} argument on {@link Connection.executeIterator | executeIterator}.
   */
  public get type(): ResultType {
    return this.resultInterator.type;
  }
  /**
   * Returns true if ResultIterator is closed, false otherwise.
   */
  public get isClosed(): boolean {
    return this.resultInterator.isClosed;
  }

  // TS interface for the return value is incorrect (`value` should not be present when `done` is true)
  public next(): { done: true } | { done: false; value: T } {
    const row = this.fetchRow();
    if (row === null) {
      return { done: true };
    }
    return {
      value: row,
      done: false,
    };
  }
  public [Symbol.iterator](): this {
    return this;
  }
}
