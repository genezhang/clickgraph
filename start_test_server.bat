@echo off
cd /d C:\Users\GenZ\clickgraph
set CLICKHOUSE_URL=http://localhost:8123
set CLICKHOUSE_USER=test_user
set CLICKHOUSE_PASSWORD=test_pass
set CLICKHOUSE_DATABASE=brahmand
set GRAPH_CONFIG_PATH=tests/integration/test_integration.yaml

echo Starting ClickGraph with test schema...
echo GRAPH_CONFIG_PATH=%GRAPH_CONFIG_PATH%
echo.

target\release\clickgraph.exe --http-port 8080 --disable-bolt
