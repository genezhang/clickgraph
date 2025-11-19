# Integration Test Runner for Windows
# Sets up proper environment variables and runs pytest

# ClickHouse connection settings
$env:CLICKHOUSE_HOST = "localhost"
$env:CLICKHOUSE_PORT = "8123"
$env:CLICKHOUSE_USER = "test_user"
$env:CLICKHOUSE_PASSWORD = "test_pass"

# ClickGraph API endpoint
$env:CLICKGRAPH_URL = "http://localhost:8080"

Write-Host "🔧 Integration Test Environment Setup" -ForegroundColor Cyan
Write-Host "  ClickHouse: ${env:CLICKHOUSE_USER}@${env:CLICKHOUSE_HOST}:${env:CLICKHOUSE_PORT}" -ForegroundColor Gray
Write-Host "  ClickGraph: ${env:CLICKGRAPH_URL}" -ForegroundColor Gray
Write-Host ""

# Check if server is running
try {
    $response = Invoke-RestMethod -Uri "${env:CLICKGRAPH_URL}/health" -TimeoutSec 2
    Write-Host "✅ ClickGraph server is running (version: $($response.version))" -ForegroundColor Green
} catch {
    Write-Host "❌ ClickGraph server is not running!" -ForegroundColor Red
    Write-Host "   Start the server first with: cargo run --release --bin clickgraph" -ForegroundColor Yellow
    exit 1
}

# Check ClickHouse connection
try {
    python -c "import clickhouse_connect; client = clickhouse_connect.get_client(host='$env:CLICKHOUSE_HOST', port=$env:CLICKHOUSE_PORT, username='$env:CLICKHOUSE_USER', password='$env:CLICKHOUSE_PASSWORD'); client.command('SELECT 1')" 2>&1 | Out-Null
    Write-Host "✅ ClickHouse connection successful" -ForegroundColor Green
} catch {
    Write-Host "❌ Cannot connect to ClickHouse!" -ForegroundColor Red
    Write-Host "   Check if ClickHouse is running: docker ps | Select-String clickhouse" -ForegroundColor Yellow
    exit 1
}

Write-Host ""
Write-Host "🧪 Running Integration Tests..." -ForegroundColor Cyan
Write-Host ""

# Run pytest with arguments passed to script
$pytestArgs = $args
if ($pytestArgs.Count -eq 0) {
    # Default: run all tests with summary
    python -m pytest tests/integration/ -v --tb=short
} else {
    # Run with custom arguments
    python -m pytest @pytestArgs
}

$exitCode = $LASTEXITCODE

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "✅ All tests passed!" -ForegroundColor Green
} else {
    Write-Host "⚠️ Some tests failed (exit code: $exitCode)" -ForegroundColor Yellow
}

exit $exitCode
