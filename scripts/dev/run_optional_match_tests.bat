@echo off
REM Quick test runner - assumes environment is already set up
REM For complete setup, run: .\setup_and_test.bat

cd /d %~dp0

REM Set environment variables
set CLICKHOUSE_URL=http://localhost:8123
set CLICKHOUSE_USER=test_user
set CLICKHOUSE_PASSWORD=test_pass
set CLICKHOUSE_DATABASE=test_integration
set GRAPH_CONFIG_PATH=tests/integration/test_integration.yaml

REM Check if server is running
curl -s http://localhost:8080/health >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: ClickGraph server is not running!
    echo Run setup first: .\setup_and_test.bat
    exit /b 1
)

REM Run tests
cd tests\integration
echo Running OPTIONAL MATCH tests...
pytest test_optional_match.py -v %*
