# Quick Wiki Validation Setup
# Run this script to prepare for wiki validation

Write-Host "`n🚀 ClickGraph Wiki Validation Setup`n" -ForegroundColor Cyan

# Check prerequisites
Write-Host "📋 Checking prerequisites..." -ForegroundColor Yellow

# Check if ClickHouse is running
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8123/ping" -Method GET -TimeoutSec 3 -ErrorAction Stop
    Write-Host "  ✅ ClickHouse is running" -ForegroundColor Green
} catch {
    Write-Host "  ❌ ClickHouse is NOT running" -ForegroundColor Red
    Write-Host "     Start it with: docker-compose up -d" -ForegroundColor Yellow
    exit 1
}

# Check if Python is available
try {
    $pythonVersion = python --version 2>&1
    Write-Host "  ✅ Python installed: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "  ❌ Python is NOT installed" -ForegroundColor Red
    exit 1
}

# Check if requests library is installed
try {
    python -c "import requests" 2>&1 | Out-Null
    Write-Host "  ✅ Python requests library installed" -ForegroundColor Green
} catch {
    Write-Host "  ⚠️  Python requests library NOT installed" -ForegroundColor Yellow
    Write-Host "     Installing..." -ForegroundColor Yellow
    pip install requests
}

# Check if benchmark schema exists
if (Test-Path "benchmarks\schemas\social_benchmark.yaml") {
    Write-Host "  ✅ Benchmark schema found" -ForegroundColor Green
} else {
    Write-Host "  ❌ Benchmark schema NOT found" -ForegroundColor Red
    Write-Host "     Expected at: benchmarks\schemas\social_benchmark.yaml" -ForegroundColor Yellow
    exit 1
}

Write-Host "`n📊 Setting up test environment..." -ForegroundColor Yellow

# Set environment variables
$env:CLICKHOUSE_URL = "http://localhost:8123"
$env:CLICKHOUSE_USER = "default"
$env:CLICKHOUSE_PASSWORD = ""
$env:CLICKHOUSE_DATABASE = "brahmand"
$env:GRAPH_CONFIG_PATH = ".\benchmarks\schemas\social_benchmark.yaml"
$env:RUST_LOG = "info"

Write-Host "  Environment variables set:" -ForegroundColor Green
Write-Host "    CLICKHOUSE_URL: $env:CLICKHOUSE_URL"
Write-Host "    CLICKHOUSE_DATABASE: $env:CLICKHOUSE_DATABASE"
Write-Host "    GRAPH_CONFIG_PATH: $env:GRAPH_CONFIG_PATH"

Write-Host "`n🔧 Build ClickGraph..." -ForegroundColor Yellow
cargo build --release 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  ✅ Build successful" -ForegroundColor Green
} else {
    Write-Host "  ❌ Build failed" -ForegroundColor Red
    exit 1
}

Write-Host "`n🚀 Starting ClickGraph server in background..." -ForegroundColor Yellow

# Start server as background job
$job = Start-Job -ScriptBlock {
    param($workDir, $configPath)
    Set-Location $workDir
    $env:CLICKHOUSE_URL = "http://localhost:8123"
    $env:CLICKHOUSE_USER = "default"
    $env:CLICKHOUSE_PASSWORD = ""
    $env:CLICKHOUSE_DATABASE = "brahmand"
    $env:GRAPH_CONFIG_PATH = $configPath
    $env:RUST_LOG = "info"
    
    cargo run --release --bin clickgraph
} -ArgumentList $PWD.Path, (Resolve-Path "benchmarks\schemas\social_benchmark.yaml").Path

Write-Host "  Server starting (Job ID: $($job.Id))..." -ForegroundColor Green
Write-Host "  Waiting for server to be ready..." -ForegroundColor Yellow

# Wait for server to be ready
$maxAttempts = 30
$attempt = 0
$serverReady = $false

while ($attempt -lt $maxAttempts -and -not $serverReady) {
    Start-Sleep -Seconds 1
    $attempt++
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8080/health" -Method GET -TimeoutSec 2 -ErrorAction Stop
        if ($response.StatusCode -eq 200) {
            $serverReady = $true
            Write-Host "  ✅ Server is ready! (took $attempt seconds)" -ForegroundColor Green
        }
    } catch {
        Write-Host "." -NoNewline -ForegroundColor Gray
    }
}

if (-not $serverReady) {
    Write-Host "`n  ❌ Server failed to start within $maxAttempts seconds" -ForegroundColor Red
    Write-Host "     Check logs with: Receive-Job -Id $($job.Id)" -ForegroundColor Yellow
    Stop-Job -Id $job.Id
    Remove-Job -Id $job.Id
    exit 1
}

Write-Host "`n✅ Setup complete! Ready for validation.`n" -ForegroundColor Green

Write-Host "📝 Next steps:" -ForegroundColor Cyan
Write-Host "  1. Run validation:" -ForegroundColor White
Write-Host "     python scripts\validate_wiki_docs.py --docs-dir docs\wiki`n" -ForegroundColor Yellow
Write-Host "  2. Check validation report:" -ForegroundColor White
Write-Host "     cat docs\WIKI_VALIDATION_REPORT.md`n" -ForegroundColor Yellow
Write-Host "  3. When done, stop server:" -ForegroundColor White
Write-Host "     Stop-Job -Id $($job.Id); Remove-Job -Id $($job.Id)`n" -ForegroundColor Yellow

Write-Host "💡 Server Job ID: $($job.Id)" -ForegroundColor Cyan
Write-Host "   Check output: Receive-Job -Id $($job.Id) -Keep`n" -ForegroundColor Gray

# Save job ID for later
$job.Id | Out-File -FilePath ".clickgraph_job_id" -Force
