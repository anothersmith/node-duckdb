# Node-DuckDB

###### [API](https://github.com/deepcrawl/node-duckdb/docs/api/node-duckdb.md) | [Code Of Conduct](https://github.com/deepcrawl/node-duckdb/docs/CODE_OF_CONDUCT.md) | [Contributing](https://github.com/deepcrawl/node-duckdb/docs/CONTRIBUTING.md) | [Developing](https://github.com/deepcrawl/node-duckdb/docs/DEVELOPING.md)

> Production ready DuckDB Node.js library written in TypeScript.
> [<img src="https://www.deepcrawl.com/wp-content/themes/deepcrawl/images/deepcrawl-logo.svg" height="200" width="300" align="right">](https://www.deepcrawl.com/)

## Overview

- This is a library that adds support for [DuckDB](https://duckdb.org/) to NodeJS.
- It comes preinstalled with DuckDB ver 0.2.2 with the **parquet** extension included.
- Has been tested to work with Linux and MacOS.
- Currently supports NodeJS v12.17.0+.
- Supports BIGINT and HUGEINT types as [BigInt](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/BigInt).
- Provides a **Promise**-based API and a **Stream**-based one.

## Getting Started

### Installation

To use Node-DuckDB in your project:

```
npm i node-duckdb
```

or

```
yarn add node-duckdb
```

Note: this will download the duckdb binary for your platform (currently Linux and MacOS are supported), or if it's not available will attempt to build it.

### Usage

Using node-duckdb is easy:

```
const db = new DuckDB();
const connection = new Connection(db);
await connection.execute("SELECT * FROM mytable;");
```

#### Promise API example

An example using promises:

```
import { Connection, DuckDB } from "node-duckdb";

async function queryDatabaseWithIterator() {
  // create new database in memory
  const db = new DuckDB();
  // create a new connection to the database
 const connection = new Connection(db);

 // perform some queries
 await connection.executeIterator("CREATE TABLE people(id INTEGER, name VARCHAR);");
 await connection.executeIterator("INSERT INTO people VALUES (1, 'Mark'), (2, 'Hannes'), (3, 'Bob');");
 const result = await connection.executeIterator("SELECT * FROM people;");

 // fetch and print result
 console.log(result.fetchAllRows());

 // release resources
 connection.close();
 db.close();
}

queryDatabaseWithIterator();

```

#### Streaming API example

A demo of reading from DuckDB, transforming to CSV and writing to file using the streaming API:

```
import { Connection, DuckDB, RowResultFormat } from "node-duckdb";
import { createWriteStream } from "fs";
import { Transform } from "stream";


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

    // result is a stream of arrays
    const resultStream = await connection.execute("SELECT * FROM people;", {rowResultFormat: RowResultFormat.Array});

    const transformToCsvStream = new ArrayToCsvTransform();
    const writeStream = createWriteStream("my-people-output");
    // objects -> csv strings -> file
    resultStream.pipe(transformToCsvStream).pipe(writeStream);
}

outputToFileAsCsv();

```

#### Complete sample project

You can see a complete sample project using node-duckdb [here](https://github.com/deepcrawl/node-duckdb/tree/master/examples).

## API

API documentation is found [here](https://github.com/deepcrawl/node-duckdb/docs/api/node-duckdb.md).

## Developing

Documentation for developers is found [here](https://github.com/deepcrawl/node-duckdb/docs/DEVELOPING.md).
