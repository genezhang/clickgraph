# Regression Test Server Startup Script
# For v0.5.2-alpha regression testing
# Handles proper environment variables and background process management

param(
    [switch]$Start,
    [switch]$Stop,
    [switch]$Status,
    [switch]$Clean
)

$ServerJobName = "ClickGraph_Regression_Server"
$ServerPort = 8080

function Start-RegressionServer {
    Write-Host "🚀 Starting ClickGraph regression test server..." -ForegroundColor Cyan
    
    # Check if already running
    $existingJob = Get-Job -Name $ServerJobName -ErrorAction SilentlyContinue
    if ($existingJob -and $existingJob.State -eq "Running") {
        Write-Host "⚠️  Server already running (Job ID: $($existingJob.Id))" -ForegroundColor Yellow
        return
    }
    
    # Clean up any stopped jobs
    Get-Job -Name $ServerJobName -ErrorAction SilentlyContinue | Remove-Job -Force
    
    # Set environment variables
    $env:CLICKHOUSE_URL = "http://localhost:8123"
    $env:CLICKHOUSE_USER = "default"
    $env:CLICKHOUSE_PASSWORD = ""  # Empty password for local development
    $env:CLICKHOUSE_DATABASE = "brahmand"
    $env:GRAPH_CONFIG_PATH = ".\benchmarks\schemas\social_benchmark.yaml"
    $env:RUST_LOG = "info"
    $env:HTTP_PORT = "8080"
    $env:BOLT_PORT = "7687"
    
    Write-Host "📝 Environment configuration:" -ForegroundColor Gray
    Write-Host "   CLICKHOUSE_URL: $env:CLICKHOUSE_URL" -ForegroundColor Gray
    Write-Host "   CLICKHOUSE_USER: $env:CLICKHOUSE_USER" -ForegroundColor Gray
    Write-Host "   CLICKHOUSE_DATABASE: $env:CLICKHOUSE_DATABASE" -ForegroundColor Gray
    Write-Host "   GRAPH_CONFIG_PATH: $env:GRAPH_CONFIG_PATH" -ForegroundColor Gray
    Write-Host "   HTTP_PORT: $env:HTTP_PORT" -ForegroundColor Gray
    Write-Host "   BOLT_PORT: $env:BOLT_PORT" -ForegroundColor Gray
    
    # Start server in background job
    $job = Start-Job -Name $ServerJobName -ScriptBlock {
        param($url, $user, $pwd, $db, $config, $log, $http_port, $bolt_port)
        
        # Set environment in job context
        $env:CLICKHOUSE_URL = $url
        $env:CLICKHOUSE_USER = $user
        $env:CLICKHOUSE_PASSWORD = $pwd
        $env:CLICKHOUSE_DATABASE = $db
        $env:GRAPH_CONFIG_PATH = $config
        $env:RUST_LOG = $log
        $env:HTTP_PORT = $http_port
        $env:BOLT_PORT = $bolt_port
        
        # Change to project directory
        Set-Location $using:PWD
        
        # Run server
        cargo run --release --bin clickgraph
        
    } -ArgumentList $env:CLICKHOUSE_URL, $env:CLICKHOUSE_USER, $env:CLICKHOUSE_PASSWORD, `
                     $env:CLICKHOUSE_DATABASE, $env:GRAPH_CONFIG_PATH, $env:RUST_LOG, `
                     $env:HTTP_PORT, $env:BOLT_PORT
    
    Write-Host "⏳ Server starting (Job ID: $($job.Id))..." -ForegroundColor Yellow
    Write-Host "   Waiting for server to be ready..."
    
    # Wait for server to start (max 30 seconds)
    $maxAttempts = 30
    $attempt = 0
    $serverReady = $false
    
    while ($attempt -lt $maxAttempts) {
        Start-Sleep -Seconds 1
        $attempt++
        
        try {
            $response = Invoke-WebRequest -Uri "http://localhost:$ServerPort/health" -TimeoutSec 2 -UseBasicParsing
            if ($response.StatusCode -eq 200) {
                $serverReady = $true
                break
            }
        } catch {
            # Server not ready yet, continue waiting
        }
        
        # Check if job failed
        $jobState = (Get-Job -Id $job.Id).State
        if ($jobState -eq "Failed" -or $jobState -eq "Stopped") {
            Write-Host "❌ Server job failed to start" -ForegroundColor Red
            Write-Host "`n📋 Job output:" -ForegroundColor Yellow
            Receive-Job -Id $job.Id
            return
        }
    }
    
    if ($serverReady) {
        Write-Host "✅ Server is ready at http://localhost:$ServerPort" -ForegroundColor Green
        Write-Host "   Job ID: $($job.Id)" -ForegroundColor Gray
        Write-Host "`n💡 Tip: Use 'Get-Job -Id $($job.Id) | Receive-Job -Keep' to view server logs" -ForegroundColor Cyan
    } else {
        Write-Host "⚠️  Server started but health check timed out" -ForegroundColor Yellow
        Write-Host "   Check server logs with: Receive-Job -Id $($job.Id) -Keep" -ForegroundColor Yellow
    }
}

function Stop-RegressionServer {
    Write-Host "🛑 Stopping regression test server..." -ForegroundColor Cyan
    
    $job = Get-Job -Name $ServerJobName -ErrorAction SilentlyContinue
    if ($job) {
        Write-Host "   Stopping job $($job.Id)..."
        Stop-Job -Id $job.Id
        Remove-Job -Id $job.Id -Force
        Write-Host "✅ Server stopped" -ForegroundColor Green
    } else {
        Write-Host "⚠️  No server job found" -ForegroundColor Yellow
    }
}

function Get-ServerStatus {
    Write-Host "📊 Regression test server status:" -ForegroundColor Cyan
    
    $job = Get-Job -Name $ServerJobName -ErrorAction SilentlyContinue
    if ($job) {
        Write-Host "   Job ID: $($job.Id)" -ForegroundColor Gray
        Write-Host "   State: $($job.State)" -ForegroundColor Gray
        Write-Host "   Has More Data: $($job.HasMoreData)" -ForegroundColor Gray
        
        # Check HTTP endpoint
        try {
            $response = Invoke-WebRequest -Uri "http://localhost:$ServerPort/health" -TimeoutSec 2 -UseBasicParsing
            Write-Host "   HTTP Status: ✅ Responding ($($response.StatusCode))" -ForegroundColor Green
        } catch {
            Write-Host "   HTTP Status: ❌ Not responding" -ForegroundColor Red
        }
        
        # Show recent logs
        Write-Host "`n📋 Recent logs (last 10 lines):" -ForegroundColor Gray
        $logs = Receive-Job -Id $job.Id -Keep
        if ($logs) {
            $logs | Select-Object -Last 10 | ForEach-Object { Write-Host "   $_" -ForegroundColor DarkGray }
        } else {
            Write-Host "   No logs available yet" -ForegroundColor DarkGray
        }
    } else {
        Write-Host "   ⚠️  No server job running" -ForegroundColor Yellow
    }
}

function Clean-AllServers {
    Write-Host "🧹 Cleaning up all server processes..." -ForegroundColor Cyan
    
    # Stop all jobs with our name
    Get-Job -Name $ServerJobName -ErrorAction SilentlyContinue | ForEach-Object {
        Write-Host "   Removing job $($_.Id)..."
        Stop-Job -Id $_.Id
        Remove-Job -Id $_.Id -Force
    }
    
    # Kill any orphaned clickgraph processes
    $processes = Get-Process -Name "clickgraph" -ErrorAction SilentlyContinue
    if ($processes) {
        Write-Host "   Found $($processes.Count) orphaned clickgraph process(es)"
        $processes | ForEach-Object {
            Write-Host "   Killing process $($_.Id)..."
            Stop-Process -Id $_.Id -Force
        }
    }
    
    Write-Host "✅ Cleanup complete" -ForegroundColor Green
}

# Main script logic
if ($Start) {
    Start-RegressionServer
} elseif ($Stop) {
    Stop-RegressionServer
} elseif ($Status) {
    Get-ServerStatus
} elseif ($Clean) {
    Clean-AllServers
} else {
    Write-Host "ClickGraph Regression Test Server Manager" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Usage:" -ForegroundColor Yellow
    Write-Host "  .\scripts\test\start_regression_server.ps1 -Start    # Start server"
    Write-Host "  .\scripts\test\start_regression_server.ps1 -Stop     # Stop server"
    Write-Host "  .\scripts\test\start_regression_server.ps1 -Status   # Check status"
    Write-Host "  .\scripts\test\start_regression_server.ps1 -Clean    # Clean up all"
    Write-Host ""
    Write-Host "Examples:" -ForegroundColor Yellow
    Write-Host "  .\scripts\test\start_regression_server.ps1 -Start"
    Write-Host "  cd tests\integration; python -m pytest test_basic_queries.py -v"
    Write-Host "  .\scripts\test\start_regression_server.ps1 -Stop"
}
