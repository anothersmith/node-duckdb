#!/bin/sh
cd markdown
sed -i '' -- 's/\[Home\](.\/index\.md) &gt; \[node-duckdb\]/[Node-DuckDB API Overview]/g' *
