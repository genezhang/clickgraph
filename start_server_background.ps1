# ClickGraph Server Background Launcher for Windows
# This script starts the ClickGraph server as a background job
#
# Usage Examples:
#   .\start_server_background.ps1                                    # Default config
#   .\start_server_background.ps1 -ConfigPath "ecommerce_graph_demo.yaml"  # Ecommerce schema
#   .\start_server_background.ps1 -HttpPort 8081 -LogLevel "debug"  # Custom port and logging
#   .\start_server_background.ps1 -Database "test_db" -EnableBolt   # Custom database with Bolt
#   .\start_server_background.ps1 -ConfigPath "custom.yaml" -Database "my_db" -LogLevel "trace"

param(
    [int]$HttpPort = 8080,
    [int]$BoltPort = 7687,
    [string]$ConfigPath = "social_network.yaml",
    [string]$Database = "brahmand",
    [string]$ClickHouseUrl = "http://localhost:8123",
    [string]$ClickHouseUser = "test_user",
    [string]$ClickHousePassword = "test_pass",
    [string]$LogLevel = "info",
    [switch]$EnableBolt,
    [switch]$DebugBuild
)

# Set environment variables
$env:CLICKHOUSE_URL = $ClickHouseUrl
$env:CLICKHOUSE_USER = $ClickHouseUser
$env:CLICKHOUSE_PASSWORD = $ClickHousePassword
$env:CLICKHOUSE_DATABASE = $Database
$env:GRAPH_CONFIG_PATH = $ConfigPath
$env:RUST_LOG = $LogLevel

Write-Host "Starting ClickGraph server in background..." -ForegroundColor Green
Write-Host "HTTP Port: $HttpPort" -ForegroundColor Cyan
Write-Host "Bolt Port: $BoltPort" -ForegroundColor Cyan
Write-Host "Config: $ConfigPath" -ForegroundColor Cyan
Write-Host "Database: $Database" -ForegroundColor Cyan
Write-Host "ClickHouse: $ClickHouseUrl" -ForegroundColor Cyan
Write-Host "Log Level: $LogLevel" -ForegroundColor Cyan
if ($EnableBolt) { Write-Host "Bolt Protocol: Enabled" -ForegroundColor Cyan }
if ($DebugBuild) { Write-Host "Build: Debug" -ForegroundColor Cyan }

# Start the server as a background job
$job = Start-Job -ScriptBlock {
    param($httpPort, $boltPort, $configPath, $database, $clickHouseUrl, $clickHouseUser, $clickHousePassword, $logLevel, $enableBolt, $debugBuild)

    # Set environment variables in the job
    $env:CLICKHOUSE_URL = $clickHouseUrl
    $env:CLICKHOUSE_USER = $clickHouseUser
    $env:CLICKHOUSE_PASSWORD = $clickHousePassword
    $env:CLICKHOUSE_DATABASE = $database
    $env:GRAPH_CONFIG_PATH = $configPath
    $env:RUST_LOG = $logLevel

    # Change to the project directory
    Set-Location $using:PWD

    # Build cargo command
    $cargoCmd = "cargo run --bin brahmand -- --daemon --http-port $httpPort"
    if ($enableBolt) {
        $cargoCmd += " --bolt-port $boltPort"
    }
    if ($debugBuild) {
        $cargoCmd = $cargoCmd  # Already using debug build by default
    }

    # Run the server
    Invoke-Expression $cargoCmd
} -ArgumentList $HttpPort, $BoltPort, $ConfigPath, $Database, $ClickHouseUrl, $ClickHouseUser, $ClickHousePassword, $LogLevel, $EnableBolt, $DebugBuild

Write-Host "Server job started with ID: $($job.Id)" -ForegroundColor Green
Write-Host ""
Write-Host "To stop the server:" -ForegroundColor Yellow
Write-Host "  Stop-Job -Id $($job.Id); Remove-Job -Id $($job.Id)" -ForegroundColor Gray
Write-Host ""
Write-Host "To check server status:" -ForegroundColor Yellow
Write-Host "  Receive-Job -Id $($job.Id) -Keep" -ForegroundColor Gray
Write-Host "  Invoke-WebRequest -Uri 'http://localhost:$HttpPort/health'" -ForegroundColor Gray