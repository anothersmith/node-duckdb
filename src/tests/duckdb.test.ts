import { Connection, DuckDB } from "@addon";
// eslint-disable-next-line node/no-unpublished-import
import "jest-extended";

describe("DuckDB", () => {
  it("is able to close - no new connections can be created", async () => {
    const db = new DuckDB();
    await db.init();
    db.close();
    expect(() => new Connection(db)).toThrow("Database is closed");
  });

  it("is able to close - pre existing connections are closed", async () => {
    const db = new DuckDB();
    await db.init();
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
    await db.init();
    const connection = new Connection(db);
    await connection.executeIterator(query1);
    const p = connection.executeIterator(query2);
    db.close();
    await expect(p).rejects.toSatisfy((e: Error) => {
      return (
        e.message === "Database that this connection belongs to has been closed!" ||
        e.message === "INTERRUPT: Interrupted!" ||
        e.message === "INTERRUPT Error: Interrupted!"
      );
    });
    expect(db.isClosed).toBe(true);
    expect(connection.isClosed).toBe(true);
  });

  it("throws when uninitialized", () => {
    const db = new DuckDB();
    expect(() => new Connection(db)).toThrow("Database hasn't been initialized");
  });
  it("doesn't throw when initialized", async () => {
    const db = new DuckDB();
    await db.init();
    expect(() => new Connection(db)).not.toThrow();
  });
  it("throws when initialized twice", async () => {
    const db = new DuckDB();
    await db.init();
    await expect(db.init()).rejects.toThrow("Has already been initialized");
  });
});
