#!/bin/sh
cd docs/api
sed -i '' -- 's/\[Home\](.\/index\.md) &gt; \[node-duckdb\]/[Node-DuckDB API]/g' *
