/**
 * Access mode specifier
 * @public
 */
export enum AccessMode {
  Undefined = 0,
  Automatic = 1,
  ReadOnly = 2,
  ReadWrite = 3,
}
/**
 * Default sort order specifier
 * @public
 */
export enum OrderType {
  Invalid = 0,
  Default = 1,
  Ascending = 2,
  Descending = 3,
}
/**
 * Null order specifier
 * @public
 */
export enum OrderByNullType {
  Invalid = 0,
  Default = 1,
  NullsFirst = 2,
  NullsLast = 3,
}
/**
 * Result format specifier for rows
 * @public
 */
export enum RowResultFormat {
  /**
   * Object, e.g. \{name: "Bob", age: 23\}
   */
  Object = 0,
  /**
   * Array, e.g. ["Bob", 23]
   */
  Array = 1,
}
/**
 * Options object type for the DuckDB class
 * @public
 */
export interface IDuckDBOptionsConfig {
  /**
   * Access Mode
   */
  accessMode?: AccessMode;
  /**
   * Checkpoint Write Ahead Log Size (in bytes)
   */
  checkPointWALSize?: number;
  /**
   * Whether to use Direct IO
   */
  useDirectIO?: boolean;
  /**
   * Maximum memory limit for the databse (in bytes)
   */
  maximumMemory?: number;
  /**
   * Whether to use temporary directory to store data that doesn't fit in memory
   */
  useTemporaryDirectory?: boolean;
  /**
   * Location of the temporary directory
   */
  temporaryDirectory?: string;
  /**
   * Collation
   */
  collation?: string;
  /**
   * Default Order
   */
  defaultOrderType?: OrderType;
  /**
   * Default order for Null values
   */
  defaultNullOrder?: OrderByNullType;
  /**
   * Enable Copy
   */
  enableCopy?: boolean;
}
/**
 * Configuration object for DuckDB
 * @public
 */
export interface IDuckDBConfig {
  /**
   * Path to the database file. If undefined, in-memory database is created
   */
  path?: string;
  options?: IDuckDBOptionsConfig;
}
/**
 * Options for connection.execute
 * @public
 */
export interface IExecuteOptions {
  /**
   * Materialized means that the whole result is loaded into memory, as opposed to streaming which means there is a pointer to the next row and rows are retrieved one by one.
   * If falsy, DuckDB will *attempt* to not load the whole result set into memory at once.
   */
  forceMaterialized?: boolean;
  /**
   * Row format
   */
  rowResultFormat?: RowResultFormat;
}

export interface IFileSystem {
  readWithLocation: (fd: number,
    buffer: Buffer,
    length: number,
    position: number,
    callback: (buffer: Buffer) => void) => void;
  read: (fd: number,
    buffer: Buffer,
    length: number,
    callback: (buffer: Buffer) => void) => void;
  glob: (path: string, callback: (paths: string[]) => void) => void;
  getFileSize: (path: string, callback: (size: number) => void) => void; 
  openFile: (path: string, flags: number, fileLockType: number, callback: (fd: number)=>void) => void
}
