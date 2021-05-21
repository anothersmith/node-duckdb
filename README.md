# Node-DuckDB

###### [API](https://github.com/deepcrawl/node-duckdb/blob/master/docs/api/node-duckdb.md) | [Code Of Conduct](https://github.com/deepcrawl/node-duckdb/blob/master/docs/CODE_OF_CONDUCT.md) | [Contributing](https://github.com/deepcrawl/node-duckdb/blob/master/docs/CONTRIBUTING.md) | [Developing](https://github.com/deepcrawl/node-duckdb/blob/master/docs/DEVELOPING.md)

> Production ready DuckDB Node.js library written in TypeScript.
> [<img src="https://www.deepcrawl.com/wp-content/themes/deepcrawl/images/deepcrawl-logo.svg" height="200" width="300" align="right">](https://www.deepcrawl.com/)

## Overview

- This is a library that adds support for [DuckDB](https://duckdb.org/) to NodeJS.
- It comes preinstalled with DuckDB ver 0.2.6+ with the **parquet** extension included.
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

```ts
const db = new DuckDB();
const connection = new Connection(db);
await connection.execute("SELECT * FROM mytable;");
```

#### Promise API example

An example using promises:

```ts
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

Getting a stream of data from DuckDB and piping into a destination stream:

```ts
import { Connection, DuckDB } from "node-duckdb";
const db = new DuckDB();
const connection = new Connection(db);
const resultStream = await connection.execute("SELECT * FROM people;");
// get destinationStream somehow
resultStream.pipe(destinationStream);
```

#### Complete sample project

You can see a complete sample project using node-duckdb [here](https://github.com/deepcrawl/node-duckdb/tree/master/examples).

## API

API documentation is found [here](https://github.com/deepcrawl/node-duckdb/blob/master/docs/api/node-duckdb.md).

## Known Issues

Known issues and their workarounds are found [here](https://github.com/deepcrawl/node-duckdb/blob/master/docs/KNOWN_ISSUES.md).

## Developing

Documentation for developers is found [here](https://github.com/deepcrawl/node-duckdb/blob/master/docs/DEVELOPING.md).
