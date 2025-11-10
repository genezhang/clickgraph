# Start ClickGraph server in background with environment variables

$env:CLICKHOUSE_URL = "http://localhost:8123"
$env:CLICKHOUSE_USER = "test_user"
$env:CLICKHOUSE_PASSWORD = "test_pass"
$env:CLICKHOUSE_DATABASE = "test_integration"
$env:GRAPH_CONFIG_PATH = "c:\Users\GenZ\clickgraph\tests\integration\test_integration.yaml"

Write-Host "Starting ClickGraph server with environment variables..." -ForegroundColor Green
Write-Host "CLICKHOUSE_URL: $env:CLICKHOUSE_URL" -ForegroundColor Cyan
Write-Host "CLICKHOUSE_DATABASE: $env:CLICKHOUSE_DATABASE" -ForegroundColor Cyan
Write-Host "GRAPH_CONFIG_PATH: $env:GRAPH_CONFIG_PATH" -ForegroundColor Cyan

# Run the server
cargo run --bin clickgraph
