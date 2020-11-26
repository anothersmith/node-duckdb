import { Connection, DuckDB } from "@deepcrawl/node-duckdb";
import { createWriteStream } from "fs";


class MyWriteStream extends 

async function outputToFile() {
    // create new database in memory
    const db = new DuckDB();
    // create a new connection to the database
    // note you can execute only one streaming query at a time per one connection   
    const connection = new Connection(db);

    // by default a query is a streaming one, as opposed to being materialized
    await connection.execute("CREATE TABLE people(id INTEGER, name VARCHAR);");
    await connection.execute("INSERT INTO people VALUES (1, 'Mark'), (2, 'Hannes'), (3, 'Bob');");

    // result is a stream
    const result = await connection.execute("SELECT * FROM people;");

    const writeStream = createWriteStream("my-people-output");

    result.pipe(writeStream).on("close", () => {
        db.close();
    })
}

outputToFile();
