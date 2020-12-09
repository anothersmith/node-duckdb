/**
 * Specifier for how DuckDB attempts to load the result
 * @public
 */
export enum ResultType {
  /**
   * Load the whole result set into memory
   */
  Materialized = "Materialized",
  /**
   * Keep pointer to the first row, don't load the whole result set all at once
   */
  Streaming = "Streaming",
}
