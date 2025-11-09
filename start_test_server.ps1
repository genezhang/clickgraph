# Start ClickGraph server for WITH clause testing
# Uses test_integration database and schema

Write-Host "Starting ClickGraph server for WITH clause testing..." -ForegroundColor Green

# Kill any existing clickgraph process
Stop-Process -Name "clickgraph" -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# Set environment variables
$env:CLICKHOUSE_URL = "http://localhost:8123"
$env:CLICKHOUSE_USER = "test_user"
$env:CLICKHOUSE_PASSWORD = "test_pass"
$env:CLICKHOUSE_DATABASE = "test_integration"
$env:GRAPH_CONFIG_PATH = "schemas/test/test_integration_schema.yaml"
$env:RUST_LOG = "info"
$env:CLICKGRAPH_HOST = "0.0.0.0"
$env:CLICKGRAPH_PORT = "8080"
$env:CLICKGRAPH_MAX_CTE_DEPTH = "100"

Write-Host "Config:" -ForegroundColor Cyan
Write-Host "  HTTP Port: 8080" -ForegroundColor Gray
Write-Host "  Database: test_integration" -ForegroundColor Gray
Write-Host "  Schema: schemas/test/test_integration_schema.yaml" -ForegroundColor Gray
Write-Host "  ClickHouse: http://localhost:8123" -ForegroundColor Gray
Write-Host ""

# Change to brahmand directory
Set-Location brahmand

# Start server in background
$job = Start-Job -ScriptBlock {
    $env:CLICKHOUSE_URL = $using:env:CLICKHOUSE_URL
    $env:CLICKHOUSE_USER = $using:env:CLICKHOUSE_USER
    $env:CLICKHOUSE_PASSWORD = $using:env:CLICKHOUSE_PASSWORD
    $env:CLICKHOUSE_DATABASE = $using:env:CLICKHOUSE_DATABASE
    $env:GRAPH_CONFIG_PATH = $using:env:GRAPH_CONFIG_PATH
    $env:RUST_LOG = $using:env:RUST_LOG
    $env:CLICKGRAPH_HOST = $using:env:CLICKGRAPH_HOST
    $env:CLICKGRAPH_PORT = $using:env:CLICKGRAPH_PORT
    $env:CLICKGRAPH_MAX_CTE_DEPTH = $using:env:CLICKGRAPH_MAX_CTE_DEPTH
    
    Set-Location $using:PWD
    cargo run --release --bin clickgraph
}

Write-Host "Server starting (Job ID: $($job.Id))..." -ForegroundColor Yellow
Write-Host ""
Write-Host "Waiting for server to be ready..." -ForegroundColor Yellow

# Wait for server to be ready (max 30 seconds)
$maxWait = 30
$waited = 0
$ready = $false

while ($waited -lt $maxWait) {
    Start-Sleep -Seconds 1
    $waited++
    
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8080/health" -ErrorAction SilentlyContinue -TimeoutSec 1
        if ($response.StatusCode -eq 200) {
            $ready = $true
            break
        }
    } catch {
        # Server not ready yet
    }
    
    Write-Host "." -NoNewline -ForegroundColor Gray
}

Write-Host ""

if ($ready) {
    Write-Host "Server is ready!" -ForegroundColor Green
    Write-Host ""
    Write-Host "To run WITH tests:" -ForegroundColor Cyan
    Write-Host "  cd tests" -ForegroundColor Gray
    Write-Host "  python test_with_having.py" -ForegroundColor Gray
    Write-Host ""
    $jobIdStr = $job.Id.ToString()
    Write-Host "To stop the server:" -ForegroundColor Yellow
    Write-Host "  Stop-Job -Id $jobIdStr; Remove-Job -Id $jobIdStr" -ForegroundColor Gray
} else {
    Write-Host "Server failed to start within $maxWait seconds" -ForegroundColor Red
    $jobIdStr = $job.Id.ToString()
    Write-Host "Check job output:" -ForegroundColor Yellow
    Write-Host "  Receive-Job -Id $jobIdStr" -ForegroundColor Gray
    Stop-Job -Id $job.Id
    Remove-Job -Id $job.Id
}
