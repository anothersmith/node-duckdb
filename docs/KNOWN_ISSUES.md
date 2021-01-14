## Known Issues

### Cannot find `libduckdb.dylib` or `libduckdb.so`

Error like this:

```
Error: Cannot open /..../build/node-duckdb-addon.node: Error: dlopen(/..../build/node-duckdb-addon.node, 1): Library not loaded: @rpath/libduckdb.dylib
  Referenced from: /.../build/node-duckdb-addon.node
  Reason: image not found
    at Object.../../node_modules/node-duckdb/build/Release/node-duckdb-addon.node (node-duckdb-addon.node:1)
```

means that the shared library `libduckdb` can't be found, most probably this is due to a bundler you are using. Bundlers should automatically copy all required artifacts (`ncc` does that), but some of them don't. In that case you'll probably have to include an explicit step for copying the shared library over. Here's an example using `webpack`, `node-loader` and `copy-webpack-plugin`:

```
const path = require('path');
const CopyPlugin = require("copy-webpack-plugin");

module.exports = {
  // ...
  target: 'node',
  node: {
    __dirname: false,
  },
  plugins: [
    new CopyPlugin({
      patterns: [
        { from: "**/*.dylib", to: "[name].[ext]" },
      ],
    }),
  ],
  module: {
    rules: [
      {
        test: /\.node$/,
        loader: 'node-loader',
      },
    ],
  },
};
```
