import { promises as fs } from "fs";
import { join } from "path";

import { DuckDBBinding, DuckDBClass } from "@addon-bindings";
import { IDuckDBConfig, AccessMode, OrderType, OrderByNullType, IFileSystem } from "@addon-types";

/**
 * The DuckDB class represents a DuckDB database instance.
 * @public
 */
export class DuckDB {
  /**
   * Returns the current version of the node-duckdb package (from package.json)
   *
   * @remarks
   * Useful for logging/debugging
   *
   * @public
   */
  public static async getBindingsVersion(): Promise<string> {
    return JSON.parse(await fs.readFile(join(__dirname, "../../package.json"), { encoding: "utf-8" })).version;
  }
  private duckdb: DuckDBClass;
  /**
   * Represents a native instance of DuckDB.
   * @param config - optional configuration object of type {@link IDuckDBConfig | IDuckDBConfig}.
   *
   *
   * @example
   * Initializing a duckdb database in memory:
   * ```ts
   * import { DuckDB } from "node-duckdb";
   * const db = new DuckDB();
   * ```
   *
   * @example
   * Initializing a duckdb database from file:
   * ```ts
   * import { DuckDB } from "node-duckdb";
   * const db = new DuckDB({ path: join(__dirname, "./mydb") });
   * ```
   *
   * @example
   * Initializing a duckdb database from file and setting some additional options:
   * ```ts
   * import { DuckDB, OrderType } from "node-duckdb";
   * const db = new DuckDB({ path: join(__dirname, "./mydb"), options: { defaultOrderType: OrderType.Descending, temporaryDirectory: false } });
   * ```
   *
   * @public
   */
  constructor(config: IDuckDBConfig = {}, fileSystem?: IFileSystem) {
    this.duckdb = new DuckDBBinding(config, fileSystem);
  }
  /**
   * Closes the underlying duckdb database, frees associated memory and renders it unusuable.
   * @remarks
   * Even though GC will automatically destroy the Database object at some point, DuckDB data is stored in the native address space, not the V8 heap, meaning you can easily have a Node.js process taking gigabytes of memory (more than the default heap size for Node.js) with V8 not triggering GC. So, definitely think about manually calling `close()`.
   * @public
   */
  public close(): void {
    return this.duckdb.close();
  }

  public init(): Promise<void> {
    // eslint-disable-next-line 
    return new Promise((resolve, _reject) => {
      this.duckdb.init(resolve);
    });
  }
  /**
   * Returns underlying binding instance.
   * @internal
   */
  public get db(): DuckDBClass {
    return this.duckdb;
  }
  /**
   * Returns true if the underlying database has been closed, false otherwise.
   * @public
   */
  public get isClosed(): boolean {
    return this.duckdb.isClosed;
  }
  /**
   * Returns the {@link AccessMode | access mode} used by the database.
   * @public
   */
  public get accessMode(): AccessMode {
    return this.duckdb.accessMode;
  }
  /**
   * Returns the checkpoint write ahead log size used by the database.
   * @public
   */
  public get checkPointWALSize(): number {
    return this.duckdb.checkPointWALSize;
  }
  /**
   * Returns the maximum memory limit for the database.
   * @public
   */
  public get maximumMemory(): number {
    return this.duckdb.maximumMemory;
  }
  /**
   * Returns true if the database uses a temporary directory for storing data that does not fit into memory, false otherwise.
   * @public
   */
  public get useTemporaryDirectory(): boolean {
    return this.duckdb.useTemporaryDirectory;
  }
  /**
   * Returns the temporary directory location for the database.
   * @public
   */
  public get temporaryDirectory(): string {
    return this.duckdb.temporaryDirectory;
  }
  /**
   * Returns the collation used by the database.
   * @public
   */
  public get collation(): string {
    return this.duckdb.collation;
  }
  /**
   * Returns the default {@link OrderType | sort order}.
   * @public
   */
  public get defaultOrderType(): OrderType {
    return this.duckdb.defaultOrderType;
  }
  /**
   * Returns the default {@link OrderByNullType | sort order for null values}.
   * @public
   */
  public get defaultNullOrder(): OrderByNullType {
    return this.duckdb.defaultNullOrder;
  }
  /**
   * Returns true of copying is enabled, false otherwise.
   * @public
   */
  public get enableCopy(): boolean {
    return this.duckdb.enableCopy;
  }
}
