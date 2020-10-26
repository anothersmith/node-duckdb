/* eslint-disable */
import { ConnectionWrapper} from "./addon-wrapper";

// it("can do a csv scan - count", async () => {
//     const cw = new ConnectionWrapper();
//     let rw: any;
//     cw.execute("SELECT count(*) FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')", (...args: any) => {
//         console.log(args);
//         rw = args[0];
//         console.log(rw.fetchRow())
//         console.log(new Date())
//         // expect(rw.fetchRow()).toMatchObject([60]);
//         // expect(rw.fetchRow()).toBe(null);
//         cw.execute("SELECT count(*) FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')", (...args: any) => {
//             console.log(args);
//             rw = args[0];
//             console.log(rw.fetchRow())
//             console.log(new Date())
//             // expect(rw.fetchRow()).toMatchObject([60]);
//             // expect(rw.fetchRow()).toBe(null);
//         });
//     });

//     await new Promise(() => {});
//   });
jest.setTimeout(99999999999)
it("slow test", async () => {
    setInterval(() => {
    console.log(new Date());
    }, 0)
    console.log("start")
    console.log(new Date())
    const cw = new ConnectionWrapper();
    await new Promise(res => {
        cw.execute(
            "CREATE TABLE test (a INTEGER, b INTEGER);", () => {
                console.log("----herexxx")
                cw.execute(
                    "INSERT INTO test SELECT a, b FROM (VALUES (11, 22), (13, 22), (12, 21)) tbl1(a,b), repeat(0, 1000000000) tbl2(c)", 
                    () => {
        
                        console.log("end")
                        console.log(new Date())
                        res();
                    }
                    );
            });
    });


});
