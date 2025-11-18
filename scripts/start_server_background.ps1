# Start ClickGraph server in background using PowerShell job
# This avoids the Windows issue where servers exit when script ends

$ErrorActionPreference = "Stop"

Write-Host "Starting ClickGraph server in background..." -ForegroundColor Cyan

# Kill any existing clickgraph processes
Write-Host "Checking for existing clickgraph processes..." -ForegroundColor Yellow
Get-Process | Where-Object { $_.ProcessName -like "*clickgraph*" } | ForEach-Object {
    Write-Host "  Stopping process: $($_.ProcessName) (PID: $($_.Id))" -ForegroundColor Yellow
    Stop-Process -Id $_.Id -Force
}
Start-Sleep -Seconds 2

# Start the server in a background job
$job = Start-Job -ScriptBlock {
    param($workDir, $clickhouseUrl, $user, $password, $database, $configPath)
    
    Set-Location $workDir
    
    # Set environment variables in the job context
    $env:CLICKHOUSE_URL = $clickhouseUrl
    $env:CLICKHOUSE_USER = $user
    $env:CLICKHOUSE_PASSWORD = $password
    $env:CLICKHOUSE_DATABASE = $database
    $env:GRAPH_CONFIG_PATH = $configPath
    $env:RUST_LOG = "info"
    
    # Run the server (this will keep running in the background job)
    .\target\release\clickgraph.exe --http-port 8080 --bolt-port 7687
} -ArgumentList $PWD, "http://localhost:8123", "default", "", "brahmand", ".\benchmarks\schemas\social_benchmark.yaml"

# Wait for server to start
Write-Host "Waiting for server to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 3

# Check if job is running
$jobState = (Get-Job -Id $job.Id).State
if ($jobState -eq "Running") {
    Write-Host "Server started successfully!" -ForegroundColor Green
    Write-Host "  Job ID: $($job.Id)" -ForegroundColor Cyan
    Write-Host "  HTTP API: http://localhost:8080" -ForegroundColor Cyan
    Write-Host "  Bolt Protocol: bolt://localhost:7687" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "To view server output:" -ForegroundColor Yellow
    Write-Host "  Receive-Job -Id $($job.Id) -Keep" -ForegroundColor White
    Write-Host ""
    Write-Host "To stop the server:" -ForegroundColor Yellow
    Write-Host "  Stop-Job -Id $($job.Id); Remove-Job -Id $($job.Id)" -ForegroundColor White
    
    # Save job ID for later reference
    $job.Id | Out-File -FilePath ".clickgraph_job_id.txt" -Force
    Write-Host ""
    Write-Host "Job ID saved to .clickgraph_job_id.txt" -ForegroundColor Gray
} else {
    Write-Host "Server failed to start!" -ForegroundColor Red
    Write-Host "Job state: $jobState" -ForegroundColor Red
    Write-Host ""
    Write-Host "Job output:" -ForegroundColor Yellow
    Receive-Job -Id $job.Id
    Remove-Job -Id $job.Id
    exit 1
}
