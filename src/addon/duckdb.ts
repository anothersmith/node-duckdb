import { IDuckDBConfig } from "@addon-types";
import { DuckDBBinding, DuckDBClass } from "../addon-bindings";
import {AccessMode, OrderType, OrderByNullType} from "@addon-types";

/**
 * The DuckDB class represents a DuckDB database instance.
 * @public
 */
export class DuckDB {
    private duckdb: DuckDBClass;
    /**
     * DuckDB Database constructor. When called instantiates a native instance of DuckDB.
     * @param config - optional configuration object of type {@link IDuckDBConfig | IDuckDBConfig}.
     * 
     * 
     * @example
     * Initializing a duckdb database in memory:
     * ```
     * import { DuckDB } from "node-duckdb";
     * const db = new DuckDB();
     * ```
     * 
     * @example
     * Initializing a duckdb database from file:
     * ```
     * import { DuckDB } from "node-duckdb";
     * const db = new DuckDB({ path: join(__dirname, "./mydb") });
     * ```
     * 
     * @example
     * Initializing a duckdb database from file and setting some additional options:
     * ```
     * import { DuckDB, OrderType } from "node-duckdb";
     * const db = new DuckDB({ path: join(__dirname, "./mydb"), options: { defaultOrderType: OrderType.Descending, temporaryDirectory: false } });
     * ```
     * 
     * @public
     */
    constructor(config: IDuckDBConfig = {}) {
        this.duckdb = new DuckDBBinding(config);
    }
    /**
     * Closes the underlying duckdb database, frees associated memory and renders it unusuable.
     * @remarks
     * Even though GC will automatically destroy the Connection object at some point, DuckDB data is stored in the native address space, not the V8 heap, meaning you can easily have a Node.js process taking gigabytes of memory (more than the default heap size for Node.js) with V8 not triggering GC. So, definitely think about manually calling `close()`.
     * @public
     */
    public close(): void {
        return this.duckdb.close();
    }
    /**
     * Returns underlying binding instance.
     * @internal
     */
    public get db() {
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
    public get accessMode() {
        return this.duckdb.accessMode;
    }
    /**
     * Returns the checkpoint write ahead log size used by the database.
     * @public
     */
    public get checkPointWALSize() {
        return this.duckdb.checkPointWALSize;
    }
    /**
     * Returns the maximum memory limit for the database.
     * @public
     */
    public get maximumMemory() {
        return this.duckdb.maximumMemory;
    }
    /**
     * Returns true if the database uses a temporary directory for storing data that does not fit into memory, false otherwise.
     * @public
     */
    public get useTemporaryDirectory() {
        return this.duckdb.useTemporaryDirectory;
    }
    /**
     * Returns the temporary directory location for the database.
     * @public
     */
    public get temporaryDirectory() {
        return this.duckdb.temporaryDirectory;
    }
    /**
     * Returns the collation used by the database.
     * @public
     */
    public get collation() {
        return this.duckdb.collation;
    }
    /**
     * Returns the default {@link OrderType | sort order}.
     * @public
     */
    public get defaultOrderType() {
        return this.duckdb.defaultOrderType;
    }
    /**
     * Returns the default {@link OrderByNullType | sort order for null values}.
     * @public
     */
    public get defaultNullOrder() {
        return this.duckdb.defaultNullOrder;
    }
    /**
     * Returns true of copying is enabled, false otherwise.
     * @public
     */
    public get enableCopy() {
        return this.duckdb.enableCopy;
    }
}
