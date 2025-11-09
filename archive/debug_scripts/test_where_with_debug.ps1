# Test WHERE clause with debug output
# This script starts the server, runs the test, and shows debug output

Write-Host "=== Testing WHERE Clause Filter Injection ===" -ForegroundColor Cyan
Write-Host ""

# Stop any existing server
Write-Host "Stopping existing servers..." -ForegroundColor Yellow
Get-Job | Where-Object {$_.Name -like "*clickgraph*" -or $_.Command -like "*clickgraph*"} | Stop-Job -ErrorAction SilentlyContinue
Get-Job | Where-Object {$_.Name -like "*clickgraph*" -or $_.Command -like "*clickgraph*"} | Remove-Job -Force -ErrorAction SilentlyContinue
Get-Process | Where-Object {$_.ProcessName -eq "clickgraph"} | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 1

# Start server with integration test config
Write-Host "Starting server with integration test schema..." -ForegroundColor Green
& .\scripts\server\start_server_background.ps1 `
    -HttpPort 8081 `
    -ConfigPath "tests/integration/test_integration.yaml" `
    -Database "brahmand" `
    -LogLevel "debug" `
    -DisableBolt

# Wait for server to start
Write-Host "Waiting for server to initialize..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# Check if server is running
$serverRunning = $false
try {
    $health = Invoke-WebRequest -Uri "http://localhost:8081/health" -ErrorAction Stop
    Write-Host "✓ Server is running" -ForegroundColor Green
    Write-Host ""
    $serverRunning = $true
} catch {
    Write-Host "✗ Server failed to start!" -ForegroundColor Red
    Write-Host "Server output:" -ForegroundColor Yellow
    Get-Job | Receive-Job
}

if (-not $serverRunning) {
    Get-Job | Stop-Job -ErrorAction SilentlyContinue
    Get-Job | Remove-Job -Force -ErrorAction SilentlyContinue
    exit 1
}

# Run the test
Write-Host "Running WHERE clause test..." -ForegroundColor Cyan
python test_where_simple.py
$testExitCode = $LASTEXITCODE
Write-Host ""

# Get server debug output
Write-Host "=== Server Debug Output ===" -ForegroundColor Cyan
$jobs = Get-Job
if ($jobs) {
    Write-Host "Fetching output from job ID: $($jobs[0].Id)" -ForegroundColor Yellow
    Receive-Job -Job $jobs[0] -Keep | Select-Object -Last 100
} else {
    Write-Host "No job found!" -ForegroundColor Red
}

# Stop server
Write-Host ""
Write-Host "Stopping server..." -ForegroundColor Yellow
Get-Job | Stop-Job
Get-Job | Remove-Job -Force

Write-Host ""
if ($testExitCode -eq 0) {
    Write-Host "✓ Test passed!" -ForegroundColor Green
} else {
    Write-Host "✗ Test failed with exit code: $testExitCode" -ForegroundColor Red
}

exit $testExitCode
