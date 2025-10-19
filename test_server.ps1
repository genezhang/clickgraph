# ClickGraph Server Test Runner
# Manages server lifecycle and runs test queries

param(
    [switch]$Start,
    [switch]$Stop,
    [switch]$Test,
    [switch]$Clean,
    [string]$Query = "MATCH (u:User) RETURN u.name LIMIT 3"
)

$SERVER_PORT = 8080
$SERVER_PID_FILE = "server.pid"

function Start-Server {
    Write-Host "🚀 Starting ClickGraph server..." -ForegroundColor Cyan
    
    # Check if already running
    if (Test-Path $SERVER_PID_FILE) {
        $pid = Get-Content $SERVER_PID_FILE
        if (Get-Process -Id $pid -ErrorAction SilentlyContinue) {
            Write-Host "⚠️  Server already running (PID: $pid)" -ForegroundColor Yellow
            return
        }
    }
    
    # Set environment variables
    $env:GRAPH_CONFIG_PATH = "social_network.yaml"
    $env:CLICKHOUSE_URL = "http://localhost:8123"
    $env:CLICKHOUSE_USER = "test_user"
    $env:CLICKHOUSE_PASSWORD = "test_pass"
    $env:CLICKHOUSE_DATABASE = "social"
    
    # Start server in background
    $process = Start-Process powershell -ArgumentList @(
        "-NoProfile",
        "-Command",
        "cd '$PWD'; cargo run --bin brahmand --release -- --http-port $SERVER_PORT"
    ) -PassThru -WindowStyle Hidden
    
    $process.Id | Out-File $SERVER_PID_FILE
    
    Write-Host "⏳ Waiting for server to start..." -ForegroundColor Yellow
    $maxAttempts = 30
    $attempt = 0
    
    while ($attempt -lt $maxAttempts) {
        Start-Sleep -Seconds 1
        try {
            $response = Invoke-WebRequest -Uri "http://localhost:$SERVER_PORT/health" -Method GET -TimeoutSec 1 -ErrorAction SilentlyContinue
            Write-Host "✅ Server started successfully (PID: $($process.Id))" -ForegroundColor Green
            return
        } catch {
            $attempt++
            Write-Host "." -NoNewline
        }
    }
    
    Write-Host "`n❌ Server failed to start within 30 seconds" -ForegroundColor Red
    Stop-Server
}

function Stop-Server {
    Write-Host "🛑 Stopping ClickGraph server..." -ForegroundColor Cyan
    
    if (Test-Path $SERVER_PID_FILE) {
        $pid = Get-Content $SERVER_PID_FILE
        try {
            Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
            Write-Host "✅ Server stopped (PID: $pid)" -ForegroundColor Green
        } catch {
            Write-Host "⚠️  No running process found" -ForegroundColor Yellow
        }
        Remove-Item $SERVER_PID_FILE -ErrorAction SilentlyContinue
    } else {
        Write-Host "⚠️  No PID file found" -ForegroundColor Yellow
    }
    
    # Kill any orphaned processes on port 8080
    $processes = Get-NetTCPConnection -LocalPort $SERVER_PORT -ErrorAction SilentlyContinue
    foreach ($proc in $processes) {
        Stop-Process -Id $proc.OwningProcess -Force -ErrorAction SilentlyContinue
    }
}

function Test-Query {
    param([string]$CypherQuery)
    
    Write-Host "🧪 Testing query: $CypherQuery" -ForegroundColor Cyan
    
    $body = @{
        query = $CypherQuery
    } | ConvertTo-Json
    
    try {
        $response = Invoke-RestMethod -Method POST -Uri "http://localhost:$SERVER_PORT/query" -ContentType "application/json" -Body $body
        
        Write-Host "✅ Query succeeded!" -ForegroundColor Green
        Write-Host "Response:" -ForegroundColor White
        $response | ConvertTo-Json -Depth 10
        
    } catch {
        Write-Host "❌ Query failed!" -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red
        exit 1
    }
}

function Clean-Environment {
    Write-Host "🧹 Cleaning test environment..." -ForegroundColor Cyan
    
    # Stop server
    Stop-Server
    
    # Clean up any orphaned cargo processes
    Get-Process | Where-Object { $_.ProcessName -like "*brahmand*" -or $_.ProcessName -like "*cargo*" } | Stop-Process -Force -ErrorAction SilentlyContinue
    
    Write-Host "✅ Environment cleaned" -ForegroundColor Green
}

# Main execution
if ($Clean) {
    Clean-Environment
} elseif ($Start) {
    Start-Server
} elseif ($Stop) {
    Stop-Server
} elseif ($Test) {
    Test-Query -CypherQuery $Query
} else {
    Write-Host "ClickGraph Test Runner" -ForegroundColor Cyan
    Write-Host "Usage:" -ForegroundColor White
    Write-Host "  .\test_server.ps1 -Start              Start server in background"
    Write-Host "  .\test_server.ps1 -Stop               Stop server"
    Write-Host "  .\test_server.ps1 -Test               Run test query"
    Write-Host "  .\test_server.ps1 -Test -Query '...'  Run custom query"
    Write-Host "  .\test_server.ps1 -Clean              Clean up everything"
    Write-Host ""
    Write-Host "Example workflow:" -ForegroundColor Yellow
    Write-Host "  .\test_server.ps1 -Start"
    Write-Host "  .\test_server.ps1 -Test"
    Write-Host "  .\test_server.ps1 -Stop"
}
