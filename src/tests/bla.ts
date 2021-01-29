/* eslint-disable */
import { DuckDB, Connection, IExecuteOptions, RowResultFormat } from "index";

const executeOptions: IExecuteOptions = { rowResultFormat: RowResultFormat.Array, forceMaterialized: false };

async function bla() {
    const query1 = "CREATE TABLE test (a INTEGER, b INTEGER);";
    const query2 =
      "INSERT INTO test SELECT a, b FROM (VALUES (11, 22), (13, 22), (12, 21)) tbl1(a,b), repeat(0, 3000) tbl2(c)";
    const query3 = "SELECT * FROM test ORDER BY a ASC;";
    console.log("-----aaa")
    const db1 = new DuckDB();
    const db2 = new DuckDB();
    console.log("-----aaa")
    const connection1 = new Connection(db1);
    const connection2 = new Connection(db2);
    console.log("-----bbb")
    await Promise.all([
      connection1.executeIterator(query1, executeOptions),
      connection2.executeIterator(query1, executeOptions),
    ]);
    console.log("-----ccc")
    await Promise.all([
      connection1.executeIterator(query2, executeOptions),
      connection2.executeIterator(query2, executeOptions),
    ]);
    console.log("-----ddd")
    const [result1, result2] = await Promise.all([
      connection1.executeIterator(query3, executeOptions),
      connection2.executeIterator(query3, executeOptions),
    ]);
    console.log("-----eee")
    console.log(result1.fetchRow());
    console.log("-----ggg")
    console.log(result2.fetchRow());
    console.log("-----fff")
    connection1.close();
    connection2.close();
    db1.close();
    db2.close();
    console.log("-----hhhh")
    connection1.close();
    connection2.close();
    db1.close();
    db2.close();
}

bla();
