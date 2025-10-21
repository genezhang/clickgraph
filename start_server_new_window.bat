@echo off
REM Start ClickGraph server in a new window

set CLICKHOUSE_URL=http://localhost:8123
set CLICKHOUSE_USER=test_user
set CLICKHOUSE_PASSWORD=test_pass
set CLICKHOUSE_DATABASE=test_multi_rel
set GRAPH_CONFIG_PATH=multi_rel_test.yaml
set RUST_LOG=debug

echo.
echo ==========================================
echo  Starting ClickGraph Server in New Window
echo ==========================================
echo.
echo Environment:
echo   CLICKHOUSE_URL=%CLICKHOUSE_URL%
echo   GRAPH_CONFIG_PATH=%GRAPH_CONFIG_PATH%
echo   RUST_LOG=%RUST_LOG%
echo.

start "ClickGraph Server" cmd /k ".\target\debug\brahmand.exe"

echo Server started in new window!
echo You can now test queries from this terminal.
echo.
