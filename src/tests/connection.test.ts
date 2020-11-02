import { ConnectionWrapper, DuckDB } from "../index";

const errorMessage = "Must provide a valid Database object";

describe("Connection class", () => {
    it("accepts a database instance", () => {
        const db = new DuckDB();
        expect(() => new ConnectionWrapper(db)).not.toThrow();
    });

    it("throws if not given a database", () => {
        expect(() => new (<any>ConnectionWrapper)()).toThrowError(errorMessage);
    });

    it("throws if given a non-database object", () => {
        expect(() => new ConnectionWrapper({notADatabase: "true"})).toThrowError(errorMessage);
    });

    it("throws if given a primitive", () => {
        expect(() => new ConnectionWrapper(1)).toThrowError(errorMessage);
    });
})
