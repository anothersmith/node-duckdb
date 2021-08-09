import {IExecuteOptions, RowResultFormat} from "@addon-types";
import {Connection, DuckDB} from "@addon";

const executeOptions: IExecuteOptions = { rowResultFormat: RowResultFormat.Array };

describe("Http/s3 interface", () => {
    let db: DuckDB;
    let connection: Connection;
    beforeEach(() => {
        db = new DuckDB();
        connection = new Connection(db);
    });

    afterEach(() => {
        connection.close();
        db.close();
    });

    it("allows reading from https", async () => {
        console.log("----herexx");
        const result = await connection.executeIterator(
            "SELECT * FROM parquet_scan('https://github.com/deepcrawl/node-duckdb/raw/ODIN-1093-http-s3/src/tests/test-fixtures/alltypes_plain.parquet')",
            executeOptions,
        );
        console.log("----hereyy");
        expect(result.fetchRow()).toMatchObject([
            4,
            true,
            0,
            0,
            0,
            0n,
            0,
            0,
            Buffer.from("03/01/09"),
            Buffer.from("0"),
            1235865600000,
        ]);
    });
})