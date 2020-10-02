const { ConnectionWrapper, ResultWrapper } = require("bindings")(
  "node-duckdb-addon"
);

exports.ConnectionWrapper = ConnectionWrapper;
exports.ResultWrapper = ResultWrapper;
