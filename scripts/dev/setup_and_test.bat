@echo off
REM Complete test environment setup for ClickGraph integration tests
REM Run this from the project root: .\setup_and_test.bat

echo ========================================
echo ClickGraph Test Environment Setup
echo ========================================
echo.

REM Step 1: Check if ClickHouse is running
echo [1/6] Checking ClickHouse...
curl -s http://localhost:8123 >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: ClickHouse is not running!
    echo Please start it with: docker-compose up -d clickhouse-service
    exit /b 1
)
echo ✓ ClickHouse is running

REM Step 2: Create test database and tables
echo.
echo [2/6] Setting up test database...
curl -s -X POST "http://localhost:8123/?user=test_user&password=test_pass" ^
  -d "CREATE DATABASE IF NOT EXISTS test_integration"

curl -s -X POST "http://localhost:8123/?user=test_user&password=test_pass&database=test_integration" ^
  -d "CREATE TABLE IF NOT EXISTS users (id UInt32, name String, age UInt32) ENGINE = Memory"

curl -s -X POST "http://localhost:8123/?user=test_user&password=test_pass&database=test_integration" ^
  -d "CREATE TABLE IF NOT EXISTS follows (follower_id UInt32, followed_id UInt32, since String) ENGINE = Memory"

echo ✓ Database and tables created

REM Step 3: Insert test data
echo.
echo [3/6] Inserting test data...
curl -s -X POST "http://localhost:8123/?user=test_user&password=test_pass&database=test_integration" ^
  -d "TRUNCATE TABLE users"
curl -s -X POST "http://localhost:8123/?user=test_user&password=test_pass&database=test_integration" ^
  -d "TRUNCATE TABLE follows"

curl -s -X POST "http://localhost:8123/?user=test_user&password=test_pass&database=test_integration" ^
  -d "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35), (4, 'Diana', 28), (5, 'Eve', 32)"

curl -s -X POST "http://localhost:8123/?user=test_user&password=test_pass&database=test_integration" ^
  -d "INSERT INTO follows VALUES (1, 2, '2023-01-01'), (1, 3, '2023-01-15'), (2, 3, '2023-02-01'), (3, 4, '2023-02-15'), (4, 5, '2023-03-01'), (2, 4, '2023-03-15')"

echo ✓ Test data inserted

REM Step 4: Stop any running ClickGraph server
echo.
echo [4/6] Stopping existing ClickGraph server...
taskkill /F /IM clickgraph.exe >nul 2>&1
timeout /t 2 /nobreak >nul
echo ✓ Cleaned up

REM Step 5: Start ClickGraph server with proper environment
echo.
echo [5/6] Starting ClickGraph server...
cd /d %~dp0
set CLICKHOUSE_URL=http://localhost:8123
set CLICKHOUSE_USER=test_user
set CLICKHOUSE_PASSWORD=test_pass
set CLICKHOUSE_DATABASE=test_integration
set GRAPH_CONFIG_PATH=tests/integration/test_integration.yaml

echo Environment variables set:
echo   CLICKHOUSE_URL=%CLICKHOUSE_URL%
echo   CLICKHOUSE_DATABASE=%CLICKHOUSE_DATABASE%
echo   GRAPH_CONFIG_PATH=%GRAPH_CONFIG_PATH%
echo.

REM Build if needed
if not exist "target\debug\clickgraph.exe" (
    echo Building ClickGraph...
    cargo build
    if %ERRORLEVEL% NEQ 0 (
        echo ERROR: Build failed!
        exit /b 1
    )
)

REM Start server in background
start /B cmd /c "target\debug\clickgraph.exe > server.log 2>&1"

REM Wait for server to be ready
echo Waiting for server to start...
timeout /t 3 /nobreak >nul

:check_server
curl -s http://localhost:8080/health >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    timeout /t 1 /nobreak >nul
    goto check_server
)
echo ✓ ClickGraph server is ready

REM Step 6: Load schema
echo.
echo [6/6] Loading test schema...
curl -s -X POST http://localhost:8080/schemas/load ^
  -H "Content-Type: application/json" ^
  -d "{\"schema_path\": \"tests/integration/test_integration.yaml\", \"schema_name\": \"test_integration\"}"
echo.
echo ✓ Schema loaded

echo.
echo ========================================
echo Setup Complete! Ready to run tests.
echo ========================================
echo.
echo Run tests with:
echo   cd tests\integration
echo   pytest test_optional_match.py -v
echo.
echo Or run all tests:
echo   python run_tests.py
echo.
echo Server log: server.log
echo Stop server: taskkill /F /IM clickgraph.exe
echo ========================================
