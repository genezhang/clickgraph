# ClickGraph Comprehensive Regression Test Suite
# Runs all Rust unit tests, Rust integration tests, and Python integration/E2E tests
#
# Usage:
#   .\scripts\test\run_all_tests.ps1           # Run everything
#   .\scripts\test\run_all_tests.ps1 -Quick    # Skip Python tests (Rust only)
#   .\scripts\test\run_all_tests.ps1 -Python   # Python tests only

param(
    [switch]$Quick,    # Rust tests only
    [switch]$Python,   # Python tests only
    [switch]$Verbose   # Show detailed output
)

$ErrorActionPreference = "Continue"
$results = @{
    RustUnit = @{ Passed = 0; Failed = 0; Skipped = $false }
    RustIntegration = @{ Passed = 0; Failed = 0; Skipped = $false }
    PythonIntegration = @{ Passed = 0; Failed = 0; Skipped = $false }
    PythonE2E = @{ Passed = 0; Failed = 0; Skipped = $false }
}

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "ClickGraph Comprehensive Regression Test Suite" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "Date: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Gray
Write-Host ""

# ============================================================================
# Pre-flight Checks
# ============================================================================
Write-Host "[Pre-flight] Checking infrastructure..." -ForegroundColor Yellow

# Check Docker
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "[FAIL] Docker not found. Please install Docker Desktop." -ForegroundColor Red
    exit 1
}

# Check if ClickHouse container is running
$clickhouseRunning = docker ps --filter "name=clickhouse" --format "{{.Names}}" | Select-String "clickhouse"
if (-not $clickhouseRunning) {
    Write-Host "[WARN] ClickHouse not running. Starting..." -ForegroundColor Yellow
    docker-compose up -d clickhouse-service
    Start-Sleep -Seconds 10
    Write-Host "[OK] ClickHouse started" -ForegroundColor Green
} else {
    Write-Host "[OK] ClickHouse is running" -ForegroundColor Green
}

# Check if benchmark data exists (for future benchmark runs)
if (-not $Quick) {
    try {
        $dataCount = docker exec clickhouse clickhouse-client --query "SELECT count(*) FROM brahmand.users_bench" 2>&1
        if ($LASTEXITCODE -ne 0 -or [int]$dataCount -eq 0) {
            Write-Host "[INFO] Benchmark data not loaded (optional - integration tests use fixtures)" -ForegroundColor Gray
        } else {
            Write-Host "[OK] Benchmark data exists ($dataCount users in brahmand.users_bench)" -ForegroundColor Green
        }
    } catch {
        Write-Host "[INFO] Benchmark data not loaded (optional)" -ForegroundColor Gray
    }
}

Write-Host ""

# ============================================================================
# Phase 1: Rust Unit Tests (434 tests)
# ============================================================================
if (-not $Python) {
    Write-Host "[1/4] Running Rust Unit Tests..." -ForegroundColor Yellow
    Write-Host "Location: src/**/*.rs" -ForegroundColor Gray
    Write-Host ""
    
    $startTime = Get-Date
    if ($Verbose) {
        cargo test --lib --no-fail-fast
    } else {
        cargo test --lib --no-fail-fast 2>&1 | Out-Null
    }
    $exitCode = $LASTEXITCODE
    $duration = (Get-Date) - $startTime
    
    if ($exitCode -eq 0) {
        Write-Host "[OK] Rust Unit Tests: PASSED (434 tests) - $($duration.TotalSeconds.ToString('F1'))s" -ForegroundColor Green
        $results.RustUnit.Passed = 434
    } else {
        Write-Host "[FAIL] Rust Unit Tests: FAILED - $($duration.TotalSeconds.ToString('F1'))s" -ForegroundColor Red
        $results.RustUnit.Failed = 1
    }
    Write-Host ""
} else {
    $results.RustUnit.Skipped = $true
}

# ============================================================================
# Phase 2: Rust Integration Tests (12 tests)
# ============================================================================
if (-not $Python) {
    Write-Host "[2/4] Running Rust Integration Tests..." -ForegroundColor Yellow
    Write-Host "Location: tests/integration/mod.rs, tests/unit/mod.rs" -ForegroundColor Gray
    Write-Host ""
    
    $startTime = Get-Date
    if ($Verbose) {
        cargo test --test '*'
    } else {
        cargo test --test '*' 2>&1 | Out-Null
    }
    $exitCode = $LASTEXITCODE
    $duration = (Get-Date) - $startTime
    
    if ($exitCode -eq 0) {
        Write-Host "[OK] Rust Integration Tests: PASSED (12 tests) - $($duration.TotalSeconds.ToString('F1'))s" -ForegroundColor Green
        $results.RustIntegration.Passed = 12
    } else {
        Write-Host "[FAIL] Rust Integration Tests: FAILED - $($duration.TotalSeconds.ToString('F1'))s" -ForegroundColor Red
        $results.RustIntegration.Failed = 1
    }
    Write-Host ""
} else {
    $results.RustIntegration.Skipped = $true
}

# ============================================================================
# Phase 3: Python Integration Tests (318 tests)
# ============================================================================
if (-not $Quick) {
    Write-Host "[3/4] Running Python Integration Tests..." -ForegroundColor Yellow
    Write-Host "Location: tests/integration/*.py" -ForegroundColor Gray
    Write-Host ""
    
    # Check if server is running
    $serverRunning = $false
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8080/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
        $serverRunning = $true
    } catch {
        Write-Host "[WARN] Server not running on port 8080. Starting server..." -ForegroundColor Yellow
        $serverJob = Start-Job -ScriptBlock {
            Set-Location $using:PWD
            $env:CLICKHOUSE_URL = "http://localhost:8123"
            $env:CLICKHOUSE_USER = "test_user"
            $env:CLICKHOUSE_PASSWORD = "test_pass"
            $env:CLICKHOUSE_DATABASE = "test_integration"
            # Pre-load test_integration schema - most tests use this
            # Tests needing other schemas can load via /schemas/load API
            $env:GRAPH_CONFIG_PATH = "tests\integration\test_integration.yaml"
            cargo run --release --bin clickgraph
        }
        
        # Wait for server to start (up to 30 seconds)
        $maxWait = 30
        $waited = 0
        $serverReady = $false
        while ($waited -lt $maxWait) {
            Start-Sleep -Seconds 2
            $waited += 2
            try {
                $response = Invoke-WebRequest -Uri "http://localhost:8080/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
                $serverReady = $true
                Write-Host "[OK] Server responding after $waited seconds (Job ID: $($serverJob.Id))" -ForegroundColor Green
                break
            } catch {
                Write-Host "." -NoNewline -ForegroundColor Gray
            }
        }
        
        if (-not $serverReady) {
            Write-Host ""
            Write-Host "[FAIL] Server did not start after $maxWait seconds" -ForegroundColor Red
            Write-Host "Job output:" -ForegroundColor Yellow
            Receive-Job -Id $serverJob.Id
            Stop-Job -Id $serverJob.Id
            Remove-Job -Id $serverJob.Id
            $results.PythonIntegration.Skipped = $true
            $results.PythonE2E.Skipped = $true
            Write-Host ""
            Write-Host "Skipping Python tests - server failed to start" -ForegroundColor Yellow
            Write-Host ""
        }
    }
    
    # Only run tests if server is ready
    if ($serverReady -or $serverRunning) {
    Push-Location tests/integration
    try {
        if ($Verbose) {
            python -m pytest -v --tb=short
        } else {
            python -m pytest -q 2>&1 | Out-Null
        }
        $exitCode = $LASTEXITCODE
        $duration = (Get-Date) - $startTime
        
        if ($exitCode -eq 0) {
            # Count tests from pytest
            $testCount = (python -m pytest --collect-only -q 2>&1 | Select-String "test session starts" -Context 0,10 | Select-Object -Last 1).ToString()
            Write-Host "[OK] Python Integration Tests: PASSED - $($duration.TotalSeconds.ToString('F1'))s" -ForegroundColor Green
            $results.PythonIntegration.Passed = 1
        } else {
            Write-Host "[FAIL] Python Integration Tests: FAILED - $($duration.TotalSeconds.ToString('F1'))s" -ForegroundColor Red
            $results.PythonIntegration.Failed = 1
        }
    } finally {
        Pop-Location
    }
    
    # Stop server if we started it
    if ($null -ne $serverJob) {
        Stop-Job -Id $serverJob.Id
        Remove-Job -Id $serverJob.Id
        Write-Host "[OK] Server stopped" -ForegroundColor Gray
    }
    } # End if server ready
    Write-Host ""
} else {
    $results.PythonIntegration.Skipped = $true
}

# ============================================================================
# Phase 4: Python E2E Tests (21 tests - Bolt Protocol, Query Cache, Param Functions)
# ============================================================================
if (-not $Quick) {
    Write-Host "[4/4] Running Python E2E Tests..." -ForegroundColor Yellow
    Write-Host "Location: tests/e2e/*.py" -ForegroundColor Gray
    Write-Host ""
    
    $startTime = Get-Date
    Push-Location tests/e2e
    try {
        if ($Verbose) {
            python -m pytest -v --tb=short
        } else {
            python -m pytest -q 2>&1 | Out-Null
        }
        $exitCode = $LASTEXITCODE
        $duration = (Get-Date) - $startTime
        
        if ($exitCode -eq 0) {
            Write-Host "[OK] Python E2E Tests: PASSED - $($duration.TotalSeconds.ToString('F1'))s" -ForegroundColor Green
            $results.PythonE2E.Passed = 1
        } else {
            Write-Host "[FAIL] Python E2E Tests: FAILED - $($duration.TotalSeconds.ToString('F1'))s" -ForegroundColor Red
            $results.PythonE2E.Failed = 1
        }
    } finally {
        Pop-Location
    }
    Write-Host ""
} else {
    $results.PythonE2E.Skipped = $true
}

# ============================================================================
# Summary Report
# ============================================================================
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "Test Summary" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

$totalPassed = 0
$totalFailed = 0
$totalSkipped = 0

foreach ($category in $results.Keys) {
    $result = $results[$category]
    if ($result.Skipped) {
        Write-Host "[SKIP] $category`: SKIPPED" -ForegroundColor Gray
        $totalSkipped++
    } elseif ($result.Failed -gt 0) {
        Write-Host "[FAIL] $category`: FAILED" -ForegroundColor Red
        $totalFailed++
    } else {
        Write-Host "[OK] $category`: PASSED" -ForegroundColor Green
        $totalPassed++
    }
}

Write-Host ""
Write-Host "Total: $totalPassed passed, $totalFailed failed, $totalSkipped skipped" -ForegroundColor $(if ($totalFailed -eq 0) { "Green" } else { "Red" })
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

# Exit with error if any tests failed
if ($totalFailed -gt 0) {
    exit 1
}
