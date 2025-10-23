# ClickGraph Server Background Launcher for Windows
# This script starts the ClickGraph server as a background job

param(
    [int]$HttpPort = 8080,
    [int]$BoltPort = 7687,
    [string]$ConfigPath = "social_network.yaml"
)

# Set environment variables
$env:CLICKHOUSE_URL = "http://localhost:8123"
$env:CLICKHOUSE_USER = "test_user"
$env:CLICKHOUSE_PASSWORD = "test_pass"
$env:CLICKHOUSE_DATABASE = "social"
$env:GRAPH_CONFIG_PATH = $ConfigPath

Write-Host "Starting ClickGraph server in background..." -ForegroundColor Green
Write-Host "HTTP Port: $HttpPort" -ForegroundColor Cyan
Write-Host "Bolt Port: $BoltPort" -ForegroundColor Cyan
Write-Host "Config: $ConfigPath" -ForegroundColor Cyan

# Start the server as a background job
$job = Start-Job -ScriptBlock {
    param($httpPort, $boltPort, $configPath)

    # Set environment variables in the job
    $env:CLICKHOUSE_URL = "http://localhost:8123"
    $env:CLICKHOUSE_USER = "test_user"
    $env:CLICKHOUSE_PASSWORD = "test_pass"
    $env:CLICKHOUSE_DATABASE = "social"
    $env:GRAPH_CONFIG_PATH = $configPath

    # Change to the project directory
    Set-Location $using:PWD

    # Run the server with daemon flag
    & cargo run --bin brahmand -- --daemon --http-port $httpPort --bolt-port $boltPort
} -ArgumentList $HttpPort, $BoltPort, $ConfigPath

Write-Host "Server job started with ID: $($job.Id)" -ForegroundColor Green
Write-Host ""
Write-Host "To stop the server:" -ForegroundColor Yellow
Write-Host "  Stop-Job -Id $($job.Id); Remove-Job -Id $($job.Id)" -ForegroundColor Gray
Write-Host ""
Write-Host "To check server status:" -ForegroundColor Yellow
Write-Host "  Receive-Job -Id $($job.Id) -Keep" -ForegroundColor Gray
Write-Host "  Invoke-WebRequest -Uri 'http://localhost:$HttpPort/health'" -ForegroundColor Gray