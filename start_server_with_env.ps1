# Start ClickGraph Server with Environment Variables
# This script ensures environment variables are properly set before starting the server

Write-Host "`n==========================================" -ForegroundColor Cyan
Write-Host " Starting ClickGraph Server" -ForegroundColor Yellow
Write-Host "==========================================" -ForegroundColor Cyan

# Set environment variables
$env:CLICKHOUSE_URL = "http://localhost:8123"
$env:CLICKHOUSE_USER = "test_user"
$env:CLICKHOUSE_PASSWORD = "test_pass"
$env:CLICKHOUSE_DATABASE = "brahmand"
$env:GRAPH_CONFIG_PATH = "social_network.yaml"
$env:RUST_LOG = "trace"  # Enable maximum logging

# Display configuration
Write-Host "`n Environment Configuration:" -ForegroundColor Green
Write-Host "   CLICKHOUSE_URL      = $env:CLICKHOUSE_URL"
Write-Host "   CLICKHOUSE_USER     = $env:CLICKHOUSE_USER"
Write-Host "   CLICKHOUSE_DATABASE = $env:CLICKHOUSE_DATABASE"
Write-Host "   GRAPH_CONFIG_PATH   = $env:GRAPH_CONFIG_PATH"
Write-Host "   RUST_LOG            = $env:RUST_LOG"
Write-Host ""

# Start the server
Write-Host " Starting server on port 8080 (DEBUG BUILD)..." -ForegroundColor Yellow
Write-Host " (Press Ctrl+C to stop)" -ForegroundColor Gray
Write-Host ""

.\target\debug\brahmand.exe
