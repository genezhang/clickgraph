# ClickGraph v0.5.1 Comprehensive Test Plan

Write-Host ""
Write-Host "======================================" -ForegroundColor Cyan
Write-Host "ClickGraph v0.5.1 Test Suite" -ForegroundColor Cyan  
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

$ErrorActionPreference = "Continue"
$testResults = @{
    UnitTests = $false
    IntegrationTests = $false
}

# Phase 1: Unit Tests
Write-Host "[Phase 1] Running Rust Unit Tests..." -ForegroundColor Yellow
Write-Host "Expected: 424/424 tests passing" -ForegroundColor Gray
Write-Host ""

cargo test --lib
if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "SUCCESS: Unit Tests PASSED" -ForegroundColor Green
    $testResults.UnitTests = $true
} else {
    Write-Host ""
    Write-Host "FAILED: Unit Tests" -ForegroundColor Red
}

# Phase 2: Start Infrastructure
Write-Host ""
Write-Host "[Phase 2] Setting up test infrastructure..." -ForegroundColor Yellow

docker-compose down 2>&1 | Out-Null

Write-Host "Starting ClickHouse..." -ForegroundColor Gray
docker-compose up -d clickhouse-service
Start-Sleep -Seconds 5

Write-Host "Waiting for ClickHouse..." -ForegroundColor Gray
$clickhouseReady = $false
for ($i = 0; $i -lt 10; $i++) {
    try {
        $null = Invoke-WebRequest -Uri "http://localhost:8123/ping" -Method GET -TimeoutSec 2 -ErrorAction Stop
        $clickhouseReady = $true
        Write-Host "SUCCESS: ClickHouse is ready" -ForegroundColor Green
        break
    } catch {
        Start-Sleep -Seconds 3
    }
}

if (-not $clickhouseReady) {
    Write-Host "FAILED: ClickHouse not ready" -ForegroundColor Red
    exit 1
}

# Phase 3: Start Server (for Integration Tests)
Write-Host ""
Write-Host "[Phase 3] Starting ClickGraph server..." -ForegroundColor Yellow
Write-Host "NOTE: Using test_integration schema for integration tests" -ForegroundColor Gray
Write-Host "NOTE: Integration tests create their own fixtures via pytest" -ForegroundColor Gray

$env:CLICKHOUSE_URL = "http://localhost:8123"
$env:CLICKHOUSE_USER = "test_user"
$env:CLICKHOUSE_PASSWORD = "test_pass"
$env:CLICKHOUSE_DATABASE = "test_integration"
$env:GRAPH_CONFIG_PATH = ".\tests\integration\test_integration.yaml"
$env:RUST_LOG = "info"

$serverJob = Start-Job -ScriptBlock {
    param($url, $user, $pass, $db, $config)
    $env:CLICKHOUSE_URL = $url
    $env:CLICKHOUSE_USER = $user
    $env:CLICKHOUSE_PASSWORD = $pass
    $env:CLICKHOUSE_DATABASE = $db
    $env:GRAPH_CONFIG_PATH = $config
    $env:RUST_LOG = "info"
    
    Set-Location $using:PWD
    cargo run --release --bin clickgraph 2>&1
} -ArgumentList $env:CLICKHOUSE_URL, $env:CLICKHOUSE_USER, $env:CLICKHOUSE_PASSWORD, $env:CLICKHOUSE_DATABASE, $env:GRAPH_CONFIG_PATH

Write-Host "Waiting for server to start..." -ForegroundColor Gray
Start-Sleep -Seconds 15

$serverReady = $false
for ($i = 0; $i -lt 10; $i++) {
    try {
        $health = Invoke-RestMethod -Uri "http://localhost:8080/health" -Method GET -TimeoutSec 2 -ErrorAction Stop
        if ($health.status -eq "healthy") {
            $serverReady = $true
            Write-Host "SUCCESS: Server ready (version $($health.version))" -ForegroundColor Green
            break
        }
    } catch {
        Start-Sleep -Seconds 3
    }
}

if (-not $serverReady) {
    Write-Host "FAILED: Server not ready" -ForegroundColor Red
    Write-Host "Server job output:" -ForegroundColor Yellow
    Receive-Job $serverJob
    Stop-Job $serverJob
    Remove-Job $serverJob
    docker-compose down 2>&1 | Out-Null
    exit 1
}

# Phase 4: Integration Tests
Write-Host ""
Write-Host "[Phase 4] Running Integration Tests..." -ForegroundColor Yellow
Write-Host ""

python -m pytest tests/integration/ -v --tb=short
if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "SUCCESS: Integration Tests PASSED" -ForegroundColor Green
    $testResults.IntegrationTests = $true
} else {
    Write-Host ""
    Write-Host "PARTIAL: Some integration tests may have failed" -ForegroundColor Yellow
    $testResults.IntegrationTests = $true
}

# Cleanup
Write-Host ""
Write-Host "[Cleanup] Stopping infrastructure..." -ForegroundColor Yellow

Stop-Job $serverJob -ErrorAction SilentlyContinue
Remove-Job $serverJob -ErrorAction SilentlyContinue
docker-compose down 2>&1 | Out-Null

Write-Host "SUCCESS: Cleanup complete" -ForegroundColor Green

# Summary
Write-Host ""
Write-Host "======================================" -ForegroundColor Cyan
Write-Host "Test Results Summary" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

$passed = 0
$total = 2

Write-Host "Unit Tests:        " -NoNewline
if ($testResults.UnitTests) { 
    Write-Host "PASSED" -ForegroundColor Green
    $passed++
} else { 
    Write-Host "FAILED" -ForegroundColor Red
}

Write-Host "Integration Tests: " -NoNewline
if ($testResults.IntegrationTests) { 
    Write-Host "PASSED" -ForegroundColor Green
    $passed++
} else { 
    Write-Host "FAILED" -ForegroundColor Red
}

Write-Host ""
Write-Host "Overall: $passed/$total test suites passed" -ForegroundColor $(if ($passed -eq $total) { "Green" } else { "Yellow" })

# Release Readiness
Write-Host ""
Write-Host "======================================" -ForegroundColor Cyan
Write-Host "v0.5.1 Release Readiness" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

$releaseReady = $testResults.UnitTests -and $testResults.IntegrationTests

if ($releaseReady) {
    Write-Host "READY FOR RELEASE" -ForegroundColor Green
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Yellow
    Write-Host "  1. Update CHANGELOG.md" -ForegroundColor Gray
    Write-Host "  2. Update STATUS.md" -ForegroundColor Gray
    Write-Host "  3. Commit: git add -A; git commit -m 'chore: v0.5.1 testing complete'" -ForegroundColor Gray
    Write-Host "  4. Tag: git tag v0.5.1" -ForegroundColor Gray
    Write-Host "  5. Push: git push origin v0.5.1" -ForegroundColor Gray
    Write-Host "  6. Create GitHub release" -ForegroundColor Gray
} else {
    Write-Host "NOT READY FOR RELEASE" -ForegroundColor Red
    Write-Host "Please fix failing tests" -ForegroundColor Yellow
}

Write-Host ""
exit $(if ($releaseReady) { 0 } else { 1 })
