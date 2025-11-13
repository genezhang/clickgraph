# Start ClickGraph server in background using Start-Job (Windows-safe)
# This ensures the server stays running even after the script ends

$env:CLICKHOUSE_URL = "http://localhost:8123"
$env:CLICKHOUSE_USER = "default"
$env:CLICKHOUSE_PASSWORD = ""
$env:CLICKHOUSE_DATABASE = "test"

Write-Host "🚀 Starting ClickGraph server in background job..." -ForegroundColor Green

$job = Start-Job -ScriptBlock {
    param($url, $user, $pass, $db, $path)
    
    # Set environment variables in job context
    $env:CLICKHOUSE_URL = $url
    $env:CLICKHOUSE_USER = $user
    $env:CLICKHOUSE_PASSWORD = $pass
    $env:CLICKHOUSE_DATABASE = $db
    
    # Change to project directory
    Set-Location $path
    
    # Run the server
    cargo run --bin clickgraph -- --http-port 8080 --disable-bolt
} -ArgumentList $env:CLICKHOUSE_URL, $env:CLICKHOUSE_USER, $env:CLICKHOUSE_PASSWORD, $env:CLICKHOUSE_DATABASE, $PWD

Write-Host "✓ Server job started with ID: $($job.Id)" -ForegroundColor Green
Write-Host ""
Write-Host "To check server output:" -ForegroundColor Cyan
Write-Host "  Receive-Job -Id $($job.Id) -Keep" -ForegroundColor Yellow
Write-Host ""
Write-Host "To stop the server:" -ForegroundColor Cyan
Write-Host "  Stop-Job -Id $($job.Id); Remove-Job -Id $($job.Id)" -ForegroundColor Yellow
Write-Host ""
Write-Host "Waiting 5 seconds for server to start..." -ForegroundColor Cyan
Start-Sleep -Seconds 5

# Check if server started successfully
$output = Receive-Job -Id $job.Id -Keep
if ($output -match "ClickGraph server is running") {
    Write-Host "✓ Server is running and ready!" -ForegroundColor Green
} else {
    Write-Host "⚠ Server output:" -ForegroundColor Yellow
    Write-Host $output
}

Write-Host ""
Write-Host "Server job ID: $($job.Id)" -ForegroundColor Magenta
