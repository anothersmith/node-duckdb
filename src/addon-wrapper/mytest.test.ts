/* eslint-disable */
import { ConnectionWrapper} from "./addon-wrapper";

// // it("can do a csv scan - count", async () => {
// //     const cw = new ConnectionWrapper();
// //     let rw: any;
// //     cw.execute("SELECT count(*) FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')", (...args: any) => {
// //         console.log(args);
// //         rw = args[0];
// //         console.log(rw.fetchRow())
// //         console.log(new Date())
// //         // expect(rw.fetchRow()).toMatchObject([60]);
// //         // expect(rw.fetchRow()).toBe(null);
// //         cw.execute("SELECT count(*) FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')", (...args: any) => {
// //             console.log(args);
// //             rw = args[0];
// //             console.log(rw.fetchRow())
// //             console.log(new Date())
// //             // expect(rw.fetchRow()).toMatchObject([60]);
// //             // expect(rw.fetchRow()).toBe(null);
// //         });
// //     });

// //     await new Promise(() => {});
// //   });
jest.setTimeout(99999999999)
it("slow test", async () => {
    setInterval(() => {
    console.log(new Date());
    }, 0)
    console.log("start")
    console.log(new Date())
    const cw = new ConnectionWrapper();
    await cw.execute("CREATE TABLE test (a INTEGER, b INTEGER);");
    await cw.execute("INSERT INTO test SELECT a, b FROM (VALUES (11, 22), (13, 22), (12, 21)) tbl1(a,b), repeat(0, 10000000) tbl2(c)");
});

it("can do a csv scan - select all", async () => {
    const cw = new ConnectionWrapper();

    const rw = await cw.execute("SELECT * FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')");
    expect(rw.fetchRow()).toMatchObject([
      1,
      "AAAAAAAABAAAAAAA",
      873244800000,
      null,
      2450810,
      2452620,
      "Y",
      98539,
      "http://www.foo.com",
      "welcome",
      2531,
      8,
      3,
      4,
    ]);
  });
