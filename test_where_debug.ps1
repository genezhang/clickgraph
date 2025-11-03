# Test WHERE clause with debug output
Write-Host "=== Testing WHERE Clause Filter Injection ===" -ForegroundColor Cyan

# Stop any existing servers
Write-Host "Cleaning up..." -ForegroundColor Yellow
Get-Job | Stop-Job -ErrorAction SilentlyContinue
Get-Job | Remove-Job -Force -ErrorAction SilentlyContinue
Get-Process | Where-Object {$_.ProcessName -eq "clickgraph"} | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 1

# Start server
Write-Host "Starting server..." -ForegroundColor Green
& .\scripts\server\start_server_background.ps1 `
    -HttpPort 8081 `
    -ConfigPath "tests/integration/test_integration.yaml" `
    -Database "brahmand" `
    -DisableBolt

# Wait for server
Write-Host "Waiting for server..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# Run test
Write-Host "Running test..." -ForegroundColor Cyan
python test_where_simple.py

# Show debug output
Write-Host "`n=== Server Debug Output ===" -ForegroundColor Cyan
Get-Job | Receive-Job -Keep | Select-Object -Last 100

# Cleanup
Write-Host "`nCleaning up..." -ForegroundColor Yellow
Get-Job | Stop-Job
Get-Job | Remove-Job -Force
