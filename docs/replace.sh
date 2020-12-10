#!/bin/sh
cd docs/api
sed -i '' -- 's/\[Home\](.\/index\.md) &gt; \[node-duckdb\]/[Node-DuckDB API]/g' *
sed -i '' -- 's/## node-duckdb package/## Node-DuckDB API/g' node-duckdb.md
