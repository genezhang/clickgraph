#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Regression test - runs scale 1 benchmark and checks for failures
.DESCRIPTION
    This script is meant to be run in CI/CD or before releases to ensure
    no query regressions. It runs the scale 1 benchmark (fast, ~1 minute)
    and fails if any queries that previously worked now fail.
.EXAMPLE
    .\run_regression.ps1
#>

$ErrorActionPreference = "Stop"

function Log { param([string]$Msg, [string]$Color = "Cyan") Write-Host "[$(Get-Date -Format 'HH:mm:ss')] $Msg" -ForegroundColor $Color }

Log "========================================================================"
Log "ClickGraph Regression Test - Scale 1 Benchmark"
Log "========================================================================"

# Expected passing queries (as of Nov 12, 2025)
$EXPECTED_PASSING = 13
$EXPECTED_TOTAL = 13  # 3 queries disabled due to known bug

# Run benchmark
Log "[1/3] Running scale 1 benchmark..." "Yellow"
.\benchmarks\run_benchmark.ps1 -Scale 1 -Iterations 1

# Check results
Log "[2/3] Checking results..." "Yellow"
$resultFiles = Get-ChildItem "benchmarks\results\benchmark_scale1_*.json" | Sort-Object LastWriteTime -Descending | Select-Object -First 1

if (-not $resultFiles) {
    Log "❌ REGRESSION: No benchmark results found!" "Red"
    exit 1
}

$results = Get-Content $resultFiles.FullName | ConvertFrom-Json
$summary = $results.summary

Log "[3/3] Analyzing results..." "Yellow"
Log "Total Queries: $($summary.total_queries)"
Log "Passed: $($summary.passed)"
Log "Failed: $($summary.failed)"
Log "Success Rate: $($summary.success_rate)%"

# Check for regressions
$regression = $false

if ($summary.total_queries -ne $EXPECTED_TOTAL) {
    Log "❌ REGRESSION: Expected $EXPECTED_TOTAL queries, got $($summary.total_queries)" "Red"
    $regression = $true
}

if ($summary.passed -lt $EXPECTED_PASSING) {
    Log "❌ REGRESSION: Expected at least $EXPECTED_PASSING passing queries, got $($summary.passed)" "Red"
    $regression = $true
}

if ($summary.success_rate -lt 100.0) {
    Log "❌ REGRESSION: Expected 100% success rate, got $($summary.success_rate)%" "Red"
    $regression = $true
    
    # Show which queries failed
    $failedQueries = $results.results | Where-Object { $_.status -eq "FAIL" }
    Log "Failed queries:" "Red"
    foreach ($query in $failedQueries) {
        Log "  - $($query.query_name): $($query.errors[0])" "Red"
    }
}

Log "========================================================================"
if ($regression) {
    Log "❌ REGRESSION TEST FAILED" "Red"
    Log "Expected: $EXPECTED_PASSING/$EXPECTED_TOTAL passing (100%)" "Red"
    Log "Got: $($summary.passed)/$($summary.total_queries) passing ($($summary.success_rate)%)" "Red"
    exit 1
} else {
    Log "✅ REGRESSION TEST PASSED" "Green"
    Log "All $($summary.passed) queries passed successfully" "Green"
    exit 0
}
