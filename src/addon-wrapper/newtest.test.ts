import { ConnectionWrapper } from "./addon-wrapper";

      it.only("can do a csv scan - count", async () => {
        console.log("_-------xxxxx")

        const cw = new ConnectionWrapper();
        console.log("_-------yyyyy")

        const rw = await cw.execute("SELECT count(*) FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')");
        console.log("_-------11111")
        expect(await rw.fetchRow()).toMatchObject([60]);
        console.log("_-------22222")
        expect(await rw.fetchRow()).toBe(null);
        console.log("_-------333333")

      });
