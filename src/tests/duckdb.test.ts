import { Connection, DuckDB } from "@addon";
// eslint-disable-next-line node/no-unpublished-import
import "jest-extended";

describe("DuckDB", () => {
  it("is able to close - no new connections can be created", () => {
    const db = new DuckDB();
    db.close();
    expect(() => new Connection(db)).toThrow("Database is closed");
  });

  it("is able to close - pre existing connections are closed", async () => {
    const db = new DuckDB();
    const connection1 = new Connection(db);
    expect(connection1.isClosed).toBe(false);
    db.close();
    await expect(() => connection1.executeIterator("SELECT 1;")).rejects.toMatchObject({
      message: "Database that this connection belongs to has been closed!",
    });
    expect(connection1.isClosed).toBe(true);
  });

  it("is able to close - current operations are handled greacefully", async () => {
    const query1 = "CREATE TABLE test (a INTEGER, b INTEGER);";
    const query2 =
      "INSERT INTO test SELECT a, b FROM (VALUES (11, 22), (13, 22), (12, 21)) tbl1(a,b), repeat(0, 30000) tbl2(c)";
    const db = new DuckDB();
    const connection = new Connection(db);
    await connection.executeIterator(query1);
    const p = connection.executeIterator(query2);
    db.close();
    await expect(p).rejects.toSatisfy((e: Error) => {
      return (
        e.message === "Database that this connection belongs to has been closed!" ||
        e.message === "INTERRUPT: Interrupted!"
      );
    });
    expect(db.isClosed).toBe(true);
    expect(connection.isClosed).toBe(true);
  });
});
