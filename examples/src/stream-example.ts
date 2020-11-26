import { Connection, DuckDB } from "@deepcrawl/node-duckdb";
import { RowResultFormat } from "@deepcrawl/node-duckdb/dist/addon-types";
import { createWriteStream } from "fs";
import {Transform} from "stream";


class ArrayToCsvTransform extends Transform {
    constructor() {
        super({objectMode: true})
    }
    _transform(chunk: any[], _encoding: string, callback: any) {
        this.push(chunk.join(",") + '\n');
        callback();
    }
}

async function outputToFileAsCsv() {
    // create new database in memory
    const db = new DuckDB();
    // create a new connection to the database
    // note you can execute only one streaming query at a time per one connection   
    const connection = new Connection(db);

    // by default a query is a streaming one, as opposed to being materialized
    await connection.execute("CREATE TABLE people(id INTEGER, name VARCHAR);");
    await connection.execute("INSERT INTO people VALUES (1, 'Mark'), (2, 'Hannes'), (3, 'Bob');");

    // result is a stream of objects
    const resultStream = await connection.execute("SELECT * FROM people;", {rowResultFormat: RowResultFormat.Array});

    const transformToCsvStream = new ArrayToCsvTransform();
    const writeStream = createWriteStream("my-people-output");
    // objects -> csv strings -> file
    resultStream.pipe(transformToCsvStream).pipe(writeStream);
}

outputToFileAsCsv();
