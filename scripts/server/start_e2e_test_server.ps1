#!/usr/bin/env pwsh
# Start ClickGraph server for E2E integration testing with parameter+function coverage
# Uses Start-Job for proper background server execution on Windows

Write-Host ""
Write-Host "=========================================="
Write-Host " Starting ClickGraph E2E Test Server"
Write-Host "=========================================="
Write-Host ""

# Environment configuration
$env:CLICKHOUSE_URL = "http://localhost:8123"
$env:CLICKHOUSE_USER = "test_user"
$env:CLICKHOUSE_PASSWORD = "test_pass"
$env:CLICKHOUSE_DATABASE = "social"
$env:GRAPH_CONFIG_PATH = "social_network.yaml"
$env:RUST_LOG = "info"

# Check if server is already running
try {
    $response = Invoke-RestMethod -Method GET -Uri "http://localhost:8080/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
    Write-Host "✓ Server already running on port 8080" -ForegroundColor Green
    Write-Host ""
    exit 0
}
catch {
    Write-Host "Starting new server instance..." -ForegroundColor Yellow
}

# Build the release binary
Write-Host "Building ClickGraph release binary..."
cargo build --release --bin clickgraph
if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ Build failed!" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Build successful" -ForegroundColor Green
Write-Host ""

# Start server using Start-Job (proper background process handling on Windows)
Write-Host "Starting ClickGraph server in background..."
$job = Start-Job -ScriptBlock {
    param($url, $user, $password, $database, $config_path, $rust_log, $working_dir)
    
    # Set environment variables in job context
    $env:CLICKHOUSE_URL = $url
    $env:CLICKHOUSE_USER = $user
    $env:CLICKHOUSE_PASSWORD = $password
    $env:CLICKHOUSE_DATABASE = $database
    $env:GRAPH_CONFIG_PATH = $config_path
    $env:RUST_LOG = $rust_log
    
    # Change to working directory
    Set-Location $working_dir
    
    # Run the server
    & ".\target\release\clickgraph.exe" --http-port 8080
} -ArgumentList $env:CLICKHOUSE_URL, $env:CLICKHOUSE_USER, $env:CLICKHOUSE_PASSWORD, $env:CLICKHOUSE_DATABASE, $env:GRAPH_CONFIG_PATH, $env:RUST_LOG, $PWD

Write-Host "✓ Server job started (Job ID: $($job.Id))" -ForegroundColor Green
Write-Host ""

# Wait for server to be ready
Write-Host "Waiting for server to be ready..." -NoNewline
$maxAttempts = 30
$attempt = 0
$serverReady = $false

while ($attempt -lt $maxAttempts -and -not $serverReady) {
    Start-Sleep -Milliseconds 500
    try {
        $response = Invoke-RestMethod -Method GET -Uri "http://localhost:8080/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
        $serverReady = $true
    }
    catch {
        Write-Host "." -NoNewline
        $attempt++
    }
}

Write-Host ""

if ($serverReady) {
    Write-Host "✓ Server is ready!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Server Details:"
    Write-Host "  URL: http://localhost:8080"
    Write-Host "  Job ID: $($job.Id)"
    Write-Host "  Database: social"
    Write-Host "  Config: social_network.yaml"
    Write-Host ""
    Write-Host "Management Commands:"
    Write-Host "  Check output:  Receive-Job -Id $($job.Id) -Keep"
    Write-Host "  Stop server:   Stop-Job -Id $($job.Id); Remove-Job -Id $($job.Id)"
    Write-Host "  Job status:    Get-Job -Id $($job.Id)"
    Write-Host ""
    Write-Host "Ready for E2E testing!" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "✗ Server failed to start within 15 seconds" -ForegroundColor Red
    Write-Host ""
    Write-Host "Job output:"
    Receive-Job -Id $job.Id
    Stop-Job -Id $job.Id
    Remove-Job -Id $job.Id
    exit 1
}
    exit 1
}
