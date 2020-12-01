# Node-DuckDB

## Overview

- This is a library that adds support for [DuckDB](https://duckdb.org/) to NodeJS.
- It comes preinstalled with DuckDB ver 0.2.2 with the parquet extension included.
- Has been tested to work with Linux and MacOS.
- Currently supports NodeJS v12.17.0+.
- Supports BIGINT and HUGEINT types as [BigInt](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/BigInt).
- Provides an async API and a streaming one.

## Using

[See examples](examples)

## Developing

First build:

1. `yarn install` - installs dependencies including downloading duckdb
2. `yarn build:ts` - builds typescript
3. `yarn test` - runs all tests

Other useful scripts:

- `yarn build` - build everything
- `yarn build:addon` - build just the bindings code
- `yarn build:duckdb` - build just the duckdb database
- `yarn build:ts` - build just the typescript code
- `yarn lint` - lint the project
- `yarn test` - run all tests
- `yarn test csv` - run just the csv test suite

## Publishing

- `export GITHUB_TOKEN=<your PAT>` - create a PAT in github that allows uploading artifacts to github releases
- `yarn login --registry=https://npm.pkg.github.com && yarn publish` - publish will do a bunch of various stuff, including prebuilding binaries for linux/mac and publishing those
