## Developing

### First build:

Prerequisites:

- aws cli tools (for the s3 test)

1. `yarn install` - installs dependencies including downloading duckdb
2. `yarn build:ts` - builds typescript
3. `yarn test` - runs all tests

### Build using DuckDB sources:

Prerequisites:

- OpenSSL v1.1+ (on Mac you may need to specify `OPENSSL_ROOT_DIR`, e.g. ` export OPENSSL_ROOT_DIR=/usr/local/opt/openssl`)

1. `yarn download-duckdb`: downloads the version of duckdb specified in the package.json command
2. `yarn build`: will build the DuckDB sources, the C++ addon sources and the typescript sources

Other useful scripts:

- `yarn build` - build everything
- `yarn build:addon` - build just the bindings code
- `yarn build:duckdb` - build just the duckdb database
- `yarn build:ts` - build just the typescript code
- `yarn lint` - lint the project
- `yarn test` - run all tests
- `yarn test csv` - run just the csv test suite

Workflow notes:

- if addon code is changed in your PR, the package.json version should also be changed manually, otherwise the old binary will be used in CI/CD and elsewhere

## Information for internal contributors

### Important

Only allow external contributors' PR jobs to be run after inspecting the changes for any security issues (namely exposing github/appveyor tokens/secrets).

### Service Users & their Tokens

- `DeepDuckla` - GitHub/AppVeyor user.
  - GitHub credentials are found in 1Password under the name `DeepDuckla`.
  - Login to AppVeyor via GitHub.
  - User's PATs are used to run GitHub actions and AppVeyor jobs.
- `dforsber-duckdb-test` - AWS user for running s3 tests
  - Credentials in Dev IAM.
  - It's only needed to access the amazon s3 bucket, so any external user can create his own user if need be.
- `deepcrawl-tech` - NPM User
  - Credentials in 1Password
  - Used to publish to NPM

All tokens have the minimum required permissions and are exposed via GitHub secrets or AppVeyor secure variables.

### Automated Release

Once `master` is ready to be released:

- Stop merging into `master`
- In github:
  - Go to `Actions` -> `Automatic Release` -> `Run Workflow`
  - Click `Run Workflow`

This will do the following:

- Changelog will be amended and package version will be bumped using semantic commits
- The new version of `node-duckdb` with Linux binaries will be published to npm. The package will be tagged as `provisional-release` instead of `latest`, meaning that users won't get the version of the package if they just do `yarn install node-duckdb`.
- The commit and the tag will be pushed
- A github release will be created
- The commit will then be tested with the `build` job
- Appveyor will build and publish the MacOs binaries, and then run the tests

At the end you should see that:

- the `Auto Release` job has been run successfully (visible on the screen where you triggered the `Release` job)
- the `Build` job has been run successfully for the `chore(release): a.b.c` commit (visible in the github commit history)
- The Appveyor job (`continuous-integration/appveyor/branch`) has been run successfully (visible in the github commit history)

If the above checks are all green and you want to make the release a non-provisional one do:

```
yarn tag add node-duckdb@xxx latest
```

You can now continue merging into master.

### Manual Release

On a mac:

- `export GITHUB_TOKEN=<your PAT>` - create a PAT in github that allows uploading artifacts to github releases
- `yarn login && yarn publish && yarn prebuild:linux && yarn prebuild:upload` - publish will do a bunch of various stuff, including prebuilding binaries for linux/mac and publishing those
