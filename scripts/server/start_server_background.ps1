# ClickGraph Server Background Launcher for Windows
# This script starts the ClickGraph server as a background job with full environment variable configuration
#
# Usage Examples:
#   .\start_server_background.ps1                                    # Default config
#   .\start_server_background.ps1 -ConfigPath "ecommerce_graph_demo.yaml"  # Ecommerce schema
#   .\start_server_background.ps1 -HttpPort 8081 -LogLevel "debug"  # Custom port and logging
#   .\start_server_background.ps1 -Database "test_db" -DisableBolt   # Custom database, HTTP only
#   .\start_server_background.ps1 -MaxCteDepth 200 -ValidateSchema  # Custom CTE depth with validation
#   .\start_server_background.ps1 -HttpHost "127.0.0.1" -BoltHost "127.0.0.1"  # Secure binding

param(
    [int]$HttpPort = 8080,
    [int]$BoltPort = 7687,
    [string]$ConfigPath = "social_network.yaml",
    [string]$Database = "social",
    [string]$ClickHouseUrl = "http://localhost:8123",
    [string]$ClickHouseUser = "test_user",
    [string]$ClickHousePassword = "test_pass",
    [string]$LogLevel = "info",
    [string]$HttpHost = "0.0.0.0",
    [string]$BoltHost = "0.0.0.0",
    [switch]$DisableBolt,
    [int]$MaxCteDepth = 100,
    [switch]$ValidateSchema,
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
$env:CLICKGRAPH_HOST = $HttpHost
$env:CLICKGRAPH_PORT = $HttpPort.ToString()
$env:CLICKGRAPH_BOLT_HOST = $BoltHost
$env:CLICKGRAPH_BOLT_PORT = $BoltPort.ToString()
$env:CLICKGRAPH_BOLT_ENABLED = (-not $DisableBolt).ToString().ToLower()
$env:CLICKGRAPH_MAX_CTE_DEPTH = $MaxCteDepth.ToString()
$env:CLICKGRAPH_VALIDATE_SCHEMA = $ValidateSchema.ToString().ToLower()

Write-Host "Starting ClickGraph server in background..." -ForegroundColor Green
Write-Host "HTTP Port: $HttpPort (Host: $HttpHost)" -ForegroundColor Cyan
Write-Host "Bolt Port: $BoltPort (Host: $BoltHost)" -ForegroundColor Cyan
Write-Host "Config: $ConfigPath" -ForegroundColor Cyan
Write-Host "Database: $Database" -ForegroundColor Cyan
Write-Host "ClickHouse: $ClickHouseUrl" -ForegroundColor Cyan
Write-Host "Log Level: $LogLevel" -ForegroundColor Cyan
Write-Host "Max CTE Depth: $MaxCteDepth" -ForegroundColor Cyan
Write-Host "Validate Schema: $ValidateSchema" -ForegroundColor Cyan
if ($DisableBolt) {
    Write-Host "Bolt Protocol: Disabled" -ForegroundColor Yellow
} else {
    Write-Host "Bolt Protocol: Enabled" -ForegroundColor Cyan
}
if ($DebugBuild) { Write-Host "Build: Debug" -ForegroundColor Cyan }

# Start the server as a background job
$job = Start-Job -ScriptBlock {
    param($httpPort, $boltPort, $configPath, $database, $clickHouseUrl, $clickHouseUser, $clickHousePassword, $logLevel, $httpHost, $boltHost, $disableBolt, $maxCteDepth, $validateSchema, $enableBolt, $debugBuild)

    # Set environment variables in the job
    $env:CLICKHOUSE_URL = $clickHouseUrl
    $env:CLICKHOUSE_USER = $clickHouseUser
    $env:CLICKHOUSE_PASSWORD = $clickHousePassword
    $env:CLICKHOUSE_DATABASE = $database
    $env:GRAPH_CONFIG_PATH = $configPath
    $env:RUST_LOG = $logLevel
    $env:CLICKGRAPH_HOST = $httpHost
    $env:CLICKGRAPH_PORT = $httpPort.ToString()
    $env:CLICKGRAPH_BOLT_HOST = $boltHost
    $env:CLICKGRAPH_BOLT_PORT = $boltPort.ToString()
    $env:CLICKGRAPH_BOLT_ENABLED = (-not $disableBolt).ToString().ToLower()
    $env:CLICKGRAPH_MAX_CTE_DEPTH = $maxCteDepth.ToString()
    $env:CLICKGRAPH_VALIDATE_SCHEMA = $validateSchema.ToString().ToLower()

    # Change to the project directory
    Set-Location $using:PWD

    # Build cargo command (environment variables handle all configuration)
    $cargoCmd = "cargo run --bin clickgraph"
    if ($debugBuild) {
        $cargoCmd = $cargoCmd  # Already using debug build by default
    }

    # Run the server
    Invoke-Expression $cargoCmd
} -ArgumentList $HttpPort, $BoltPort, $ConfigPath, $Database, $ClickHouseUrl, $ClickHouseUser, $ClickHousePassword, $LogLevel, $HttpHost, $BoltHost, $DisableBolt, $MaxCteDepth, $ValidateSchema, $EnableBolt, $DebugBuild

Write-Host "Server job started with ID: $($job.Id)" -ForegroundColor Green
Write-Host ""
Write-Host "To stop the server:" -ForegroundColor Yellow
Write-Host "  Stop-Job -Id $($job.Id); Remove-Job -Id $($job.Id)" -ForegroundColor Gray
Write-Host ""
Write-Host "To check server status:" -ForegroundColor Yellow
Write-Host "  Receive-Job -Id $($job.Id) -Keep" -ForegroundColor Gray
Write-Host "  Invoke-WebRequest -Uri 'http://localhost:$HttpPort/health'" -ForegroundColor Gray