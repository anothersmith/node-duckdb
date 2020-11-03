export enum AccessMode {
    Automatic = 1,
    ReadOnly = 2,
    ReadWrite = 3
}

export enum OrderType {
    Ascending = 2,
    Descending = 3
}

export enum OrderByNullType {
    NullsFirst = 2,
    NullsLast = 3
}

export interface IDuckDBOptionsConfig {
    accessMode?: AccessMode;
    checkPointWALSize?: number;
    useDirectIO?: boolean;
    maximumMemory?: number;
    useTemporaryDirectory?: boolean;
    temporaryDirectory?: string;
    collation?: string;
    defaultOrderType?: OrderType;
    defaultNullOrder?: OrderByNullType;
    enableCopy?: boolean;
}

export interface IDuckDBConfig {
    path?: string;
    options?: IDuckDBOptionsConfig;
}
