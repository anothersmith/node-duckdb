# NodeJS DuckDB driver with native bindings

## Developing

- `yarn install`
- `yarn build` - builds everything, including duckdb, duckdb node bindings and the typescript wrapper
  - `yarn build:[addon|duckdb|ts]` to build one one of those things
- `yarn test` to run unit tests
- `yarn lint` to lint

## Using as a dependency

```
import { ConnectionWrapper, ResultWrapper } from "node-duckdb";
const cw = new ConnectionWrapper();
const rw = cw.execute("SELECT count(*) FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')");
rw.fetchRow()
```
