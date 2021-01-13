import { Connection, DuckDB } from "@addon";
import { AccessMode } from "@addon-types";

describe("Node filesystem", () => {
    it("allows reading from file", async () => {
        const db = new DuckDB({options: {accessMode: AccessMode.ReadWrite, useDirectIO: false}});
        const connection1 = new Connection(db);
        const result = await connection1.executeIterator(      "SELECT count(*) FROM parquet_scan('src/tests/test-fixtures/alltypes_plain.parquet')",        );
        expect(result.fetchRow()).toMatchObject({"count()": 8n});
    });
});
