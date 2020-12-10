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
- `yarn login && yarn publish` - publish will do a bunch of various stuff, including prebuilding binaries for linux/mac and publishing those
