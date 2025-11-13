#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Complete benchmark runner for ClickGraph with MergeTree tables
.EXAMPLE
    .\run_benchmark.ps1 -Scale 10 -Iterations 3
#>
param(
    [Parameter(Mandatory=$true)][int]$Scale,
    [Parameter(Mandatory=$false)][int]$Iterations = 3
)

$ErrorActionPreference = "Stop"
function Log { param([string]$Msg) Write-Host "[$(Get-Date -Format 'HH:mm:ss')] $Msg" -ForegroundColor Cyan }

Log "========================================================================"
Log "ClickGraph Benchmark Runner - Scale=$Scale, Iterations=$Iterations"
Log "========================================================================"

# 1. Generate data with MergeTree
Log "[1/4] Generating data (scale=$Scale, MergeTree)..."
python benchmarks/data/setup_unified.py --scale $Scale --engine MergeTree
if ($LASTEXITCODE -ne 0) { Log "❌ Data generation failed"; exit 1 }
Log "✅ Data generated"

# 2. Kill any existing server
Log "[2/4] Starting ClickGraph server..."
Get-Process -Name clickgraph -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 1

# 3. Start server with schema loaded at startup (the old way that worked!)
$env:CLICKHOUSE_URL = "http://localhost:8123"
$env:CLICKHOUSE_USER = "test_user"
$env:CLICKHOUSE_PASSWORD = "test_pass"
$env:CLICKHOUSE_DATABASE = "brahmand"
$env:GRAPH_CONFIG_PATH = "benchmarks\schemas\social_benchmark.yaml"
$env:RUST_LOG = "warn"

Start-Process -FilePath ".\target\release\clickgraph.exe" -WindowStyle Hidden
Start-Sleep -Seconds 3

$health = Invoke-RestMethod -Uri "http://localhost:8080/health" -ErrorAction SilentlyContinue
if ($health.status -ne "healthy") { Log "❌ Server failed to start"; exit 1 }
Log "✅ Server running"

Log "[3/4] Running benchmark suite..."
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$output = "benchmarks\results\benchmark_scale${Scale}_${timestamp}.json"

python benchmarks/queries/suite.py --scale $Scale --iterations $Iterations --output $output

Log "[4/4] Complete!"
if (Test-Path $output) {
    $results = Get-Content $output | ConvertFrom-Json
    $summary = $results.summary
    Log "========================================================================" -ForegroundColor Green
    Log "Results: $output" -ForegroundColor Cyan
    Log "Success Rate: $($summary.passed)/$($summary.total_queries) ($($summary.success_rate)%)" -ForegroundColor $(if ($summary.success_rate -ge 80) { "Green" } else { "Yellow" })
    Log "Mean Query Time: $([math]::Round(($results.results | Where-Object {$_.timing} | ForEach-Object {$_.timing.mean_ms} | Measure-Object -Average).Average, 1))ms" -ForegroundColor Cyan
    Log "========================================================================" -ForegroundColor Green
} else {
    Log "Results: $output" -ForegroundColor Yellow
}
if ($LASTEXITCODE -eq 0 -or $results.summary.success_rate -ge 80) { 
    Log "✅ Benchmark complete!" -ForegroundColor Green
} else { 
    Log "⚠️ Check results for failures" -ForegroundColor Yellow
}
