import { ConnectionWrapper } from "../index";

const query = "SELECT count(*) FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')";

describe("Streaming capability", () => {
    it("gracefully handles inactive stream", async () => {
        const cw = new ConnectionWrapper();
        const rw1 = await cw.execute(query, false);
        const rw2 = await cw.execute(query, false);

        let error = undefined;
        try {
            rw1.fetchRow();
        } catch (e) {
            error = e;
        }
        expect(error.message).toBe("No data has been returned (possibly stream has been closed: only one stream can be active on one connection at a time)")
        expect(rw2.fetchRow()).toEqual([60]);
    });
    
    it("gracefully handles inactive stream - second query is materialized", async () => {
        const cw = new ConnectionWrapper();
        const rw1 = await cw.execute(query, false);
        const rw2 = await cw.execute(query, true);

        let error = undefined;
        try {
            rw1.fetchRow();
        } catch (e) {
            error = e;
        }
        expect(error.message).toBe("No data has been returned (possibly stream has been closed: only one stream can be active on one connection at a time)")
        expect(rw2.fetchRow()).toEqual([60]);
    });

    it("work fine if done one after another", async () => {
        const cw = new ConnectionWrapper();
        const rw1 = await cw.execute(query, false);
        expect(rw1.fetchRow()).toEqual([60]);
        const rw2 = await cw.execute(query, false);
        expect(rw2.fetchRow()).toEqual([60]);
    });
})
