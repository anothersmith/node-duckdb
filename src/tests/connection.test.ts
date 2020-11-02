import { Connection, DuckDB } from "@addon";

const errorMessage = "Must provide a valid Database object";

describe("Connection class", () => {
  it("accepts a database instance", () => {
    const db = new DuckDB();
    expect(() => new Connection(db)).not.toThrow();
  });

  it("throws if not given a database", () => {
    expect(() => new (<any>Connection)()).toThrowError("Cannot read property 'db' of undefined");
  });

  it("throws if given a non-database object", () => {
    expect(() => new (<any>Connection)({ notADatabase: "true" })).toThrowError(errorMessage);
  });

  it("throws if given a primitive", () => {
    expect(() => new (<any>Connection)(1)).toThrowError(errorMessage);
  });
});
