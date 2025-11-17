# Test runner for parameterized views feature
param([switch]$SkipServerStart)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "Parameterized Views E2E Test Runner" -ForegroundColor Cyan
Write-Host ""

# Check ClickHouse
Write-Host "[1/5] Checking ClickHouse..." -ForegroundColor Yellow
$clickhouseRunning = docker ps --filter "name=clickhouse" --format "{{.Names}}" | Select-String "clickhouse"
if (-not $clickhouseRunning) {
    Write-Host "   Starting ClickHouse..." -ForegroundColor Yellow
    docker-compose up -d clickhouse-service
    Start-Sleep -Seconds 10
}
Write-Host "   ClickHouse is running" -ForegroundColor Green
Write-Host ""

# Setup test data
Write-Host "[2/5] Setting up test data..." -ForegroundColor Yellow
Get-Content tests/fixtures/data/setup_parameterized_views.sql | docker exec -i clickhouse clickhouse-client --user test_user --password test_pass --database brahmand --multiquery
if ($LASTEXITCODE -ne 0) {
    Write-Host "   Failed" -ForegroundColor Red
    exit 1
}
Write-Host "   Test data loaded" -ForegroundColor Green
Write-Host ""

# Create views
Write-Host "[3/5] Creating parameterized views..." -ForegroundColor Yellow
Get-Content tests/fixtures/data/create_parameterized_views.sql | docker exec -i clickhouse clickhouse-client --user test_user --password test_pass --database brahmand --multiquery
if ($LASTEXITCODE -ne 0) {
    Write-Host "   Failed" -ForegroundColor Red
    exit 1
}
Write-Host "   Views created" -ForegroundColor Green
Write-Host ""

# Start server
if (-not $SkipServerStart) {
    Write-Host "[4/5] Starting server..." -ForegroundColor Yellow
    
    & ".\scripts\server\start_server_background.ps1" `
        -HttpPort 8080 `
        -ConfigPath "schemas/test/multi_tenant.yaml" `
        -Database "brahmand" `
        -ClickHouseUrl "http://localhost:8123" `
        -ClickHouseUser "test_user" `
        -ClickHousePassword "test_pass" `
        -DisableBolt
    
    # Wait for health
    Write-Host "   Waiting for server..." -ForegroundColor Gray
    $maxAttempts = 30
    $attempt = 0
    $ready = $false
    
    while ($attempt -lt $maxAttempts -and -not $ready) {
        $attempt++
        Start-Sleep -Seconds 1
        
        try {
            $health = Invoke-WebRequest -Uri "http://localhost:8080/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
            if ($health.StatusCode -eq 200) {
                $ready = $true
            }
        } catch {
            # Not ready yet
        }
        
        if ($attempt % 5 -eq 0) {
            Write-Host "   ... waiting" -ForegroundColor Gray
        }
    }
    
    if (-not $ready) {
        Write-Host "   Server did not start" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "   Server ready" -ForegroundColor Green
    Write-Host ""
}

# Run tests
Write-Host "[5/5] Running tests..." -ForegroundColor Yellow
Write-Host ""
python tests/integration/test_parameterized_views_http.py
$testResult = $LASTEXITCODE

Write-Host ""
if ($testResult -eq 0) {
    Write-Host "ALL TESTS PASSED" -ForegroundColor Green
} else {
    Write-Host "TESTS FAILED" -ForegroundColor Red
}
Write-Host ""

# Cleanup
if (-not $SkipServerStart) {
    Write-Host "Stopping server..." -ForegroundColor Yellow
    Get-Job | Where-Object { $_.Command -like "*cargo run*" } | Stop-Job
    Get-Job | Where-Object { $_.Command -like "*cargo run*" } | Remove-Job
}

exit $testResult
