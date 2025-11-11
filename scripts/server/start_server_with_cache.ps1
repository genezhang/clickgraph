# Start ClickGraph Server with Query Cache Enabled
# This script ensures environment variables are properly set before starting the server

Write-Host "`n==========================================" -ForegroundColor Cyan
Write-Host " Starting ClickGraph Server (with Cache)" -ForegroundColor Yellow
Write-Host "==========================================" -ForegroundColor Cyan

# Set environment variables
$env:CLICKHOUSE_URL = "http://localhost:8123"
$env:CLICKHOUSE_USER = "test_user"
$env:CLICKHOUSE_PASSWORD = "test_pass"
$env:CLICKHOUSE_DATABASE = "brahmand"
$env:CLICKGRAPH_QUERY_CACHE_ENABLED = "true"
$env:CLICKGRAPH_QUERY_CACHE_MAX_ENTRIES = "1000"
$env:RUST_LOG = "debug"

# Display configuration
Write-Host "`n Environment Configuration:" -ForegroundColor Green
Write-Host "   CLICKHOUSE_URL                    = $env:CLICKHOUSE_URL"
Write-Host "   CLICKHOUSE_USER                   = $env:CLICKHOUSE_USER"
Write-Host "   CLICKHOUSE_DATABASE               = $env:CLICKHOUSE_DATABASE"
Write-Host "   CLICKGRAPH_QUERY_CACHE_ENABLED    = $env:CLICKGRAPH_QUERY_CACHE_ENABLED"
Write-Host "   CLICKGRAPH_QUERY_CACHE_MAX_ENTRIES= $env:CLICKGRAPH_QUERY_CACHE_MAX_ENTRIES"
Write-Host "   RUST_LOG                          = $env:RUST_LOG"
Write-Host ""

# Start the server in background using Start-Job
Write-Host " Starting server in background (RELEASE BUILD)..." -ForegroundColor Yellow

$job = Start-Job -ScriptBlock {
    param($url, $user, $pass, $db, $cache_enabled, $cache_entries, $log_level)
    
    $env:CLICKHOUSE_URL = $url
    $env:CLICKHOUSE_USER = $user
    $env:CLICKHOUSE_PASSWORD = $pass
    $env:CLICKHOUSE_DATABASE = $db
    $env:CLICKGRAPH_QUERY_CACHE_ENABLED = $cache_enabled
    $env:CLICKGRAPH_QUERY_CACHE_MAX_ENTRIES = $cache_entries
    $env:RUST_LOG = $log_level
    
    Set-Location $using:PWD
    cargo run --release --bin clickgraph
} -ArgumentList $env:CLICKHOUSE_URL, $env:CLICKHOUSE_USER, $env:CLICKHOUSE_PASSWORD, $env:CLICKHOUSE_DATABASE, $env:CLICKGRAPH_QUERY_CACHE_ENABLED, $env:CLICKGRAPH_QUERY_CACHE_MAX_ENTRIES, $env:RUST_LOG

Write-Host " Server job started with ID: $($job.Id)" -ForegroundColor Green
Write-Host " Waiting 5 seconds for server to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

Write-Host "`n To check server output:" -ForegroundColor Cyan
Write-Host "   Receive-Job -Id $($job.Id) -Keep" -ForegroundColor Gray
Write-Host "`n To stop server:" -ForegroundColor Cyan
Write-Host "   Stop-Job -Id $($job.Id); Remove-Job -Id $($job.Id)" -ForegroundColor Gray
Write-Host "`n Server should be running on http://localhost:8080" -ForegroundColor Green
Write-Host ""
