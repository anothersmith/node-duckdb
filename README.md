# NodeJS DuckDB driver with native bindings

## Developing

First build:

1. `yarn install` - installs deps including duckdb, builds duckdb and addon plugin
2. `yarn build:ts` - builds typescript
3. `yarn test` - runs all tests

Other useful scripts:

- `yarn build:[addon|duckdb|ts]` - to build one one of those things
- `yarn lint` - to lint

## Publishing
- `export GITHUB_TOKEN=<your PAT>` - create a PAT in github that allows uploading artifacts to github releases
- `yarn login --registry=https://npm.pkg.github.com && yarn publish` - publish will do a bunch of various stuff, including prebuilding binaries for linux/mac and publishing those

## Using as a dependency

```
import { Connection, DuckDB } from "@deepcrawl/node-duckdb";
let db: DuckDB = new DuckDB();
let connection: Connection = new Connection(db);
const result = await connection.executeIterator("SELECT count(*) FROM read_csv_auto('src/tests/test-fixtures/web_page.csv')");
console.log(result.fetchAllRows());
connection.close();
db.close();
```
