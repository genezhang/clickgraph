# Test Windows MergeTree Fix
# This script validates that MergeTree tables work correctly on Windows

Write-Host "🔧 Testing Windows MergeTree Fix..." -ForegroundColor Cyan

# Stop and clean existing containers
Write-Host "`n1️⃣ Stopping existing containers..." -ForegroundColor Yellow
docker-compose down -v

# Start with new configuration
Write-Host "`n2️⃣ Starting ClickHouse with named volume..." -ForegroundColor Yellow
docker-compose up -d clickhouse-service

# Wait for ClickHouse to be ready
Write-Host "`n3️⃣ Waiting for ClickHouse to be healthy..." -ForegroundColor Yellow
$maxAttempts = 30
$attempt = 0
$healthy = $false

while (-not $healthy -and $attempt -lt $maxAttempts) {
    Start-Sleep -Seconds 2
    $attempt++
    
    $healthStatus = docker inspect clickhouse --format='{{.State.Health.Status}}' 2>$null
    
    if ($healthStatus -eq "healthy") {
        $healthy = $true
        Write-Host "✅ ClickHouse is healthy!" -ForegroundColor Green
    } else {
        Write-Host "⏳ Attempt $attempt/$maxAttempts - Status: $healthStatus" -ForegroundColor Gray
    }
}

if (-not $healthy) {
    Write-Host "❌ ClickHouse failed to become healthy after $maxAttempts attempts" -ForegroundColor Red
    exit 1
}

# Test MergeTree table creation
Write-Host "`n4️⃣ Creating MergeTree test table..." -ForegroundColor Yellow

docker exec clickhouse clickhouse-client --query "CREATE TABLE IF NOT EXISTS brahmand.test_mergetree (id UInt32, name String, value Float64) ENGINE = MergeTree() ORDER BY id"

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to create MergeTree table!" -ForegroundColor Red
    exit 1
}

Write-Host "✅ MergeTree table created successfully!" -ForegroundColor Green

# Insert test data
Write-Host "`n5️⃣ Inserting test data..." -ForegroundColor Yellow

docker exec clickhouse clickhouse-client --query "INSERT INTO brahmand.test_mergetree SELECT number as id, concat('Item_', toString(number)) as name, rand() / 1000000000.0 as value FROM numbers(1000)"

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to insert data!" -ForegroundColor Red
    exit 1
}

Write-Host "✅ Inserted 1000 rows!" -ForegroundColor Green

# Verify data
Write-Host "`n6️⃣ Verifying data..." -ForegroundColor Yellow
$count = docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM brahmand.test_mergetree"

if ($count -eq "1000") {
    Write-Host "✅ Data verification passed! Count: $count" -ForegroundColor Green
} else {
    Write-Host "❌ Data verification failed! Expected 1000, got: $count" -ForegroundColor Red
    exit 1
}

# Test persistence (restart)
Write-Host "`n7️⃣ Testing data persistence after restart..." -ForegroundColor Yellow
docker-compose restart clickhouse-service

Start-Sleep -Seconds 10

$countAfterRestart = docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM brahmand.test_mergetree"

if ($countAfterRestart -eq "1000") {
    Write-Host "✅ Data persisted after restart! Count: $countAfterRestart" -ForegroundColor Green
} else {
    Write-Host "❌ Data lost after restart! Expected 1000, got: $countAfterRestart" -ForegroundColor Red
    exit 1
}

# Verify table engine
Write-Host "`n8️⃣ Verifying table engine..." -ForegroundColor Yellow
$engine = docker exec clickhouse clickhouse-client --query "SELECT engine FROM system.tables WHERE database='brahmand' AND name='test_mergetree'"

if ($engine -like "*MergeTree*") {
    Write-Host "✅ Table engine verified: $engine" -ForegroundColor Green
} else {
    Write-Host "❌ Wrong table engine: $engine" -ForegroundColor Red
    exit 1
}

# Clean up test table
Write-Host "`n9️⃣ Cleaning up test table..." -ForegroundColor Yellow
docker exec clickhouse clickhouse-client --query "DROP TABLE IF EXISTS brahmand.test_mergetree"

# Run benchmark setup with MergeTree
Write-Host "`n🔟 Testing benchmark data generation with MergeTree..." -ForegroundColor Yellow
Write-Host "Running: python tests/python/setup_benchmark_unified.py --scale 1 --engine MergeTree" -ForegroundColor Gray

python tests/python/setup_benchmark_unified.py --scale 1 --engine MergeTree

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Benchmark setup failed!" -ForegroundColor Red
    exit 1
}

Write-Host "✅ Benchmark setup successful!" -ForegroundColor Green

# Verify benchmark tables
Write-Host "`n1️⃣1️⃣ Verifying benchmark tables..." -ForegroundColor Yellow

$tables = @('users_bench', 'user_follows_bench', 'posts_bench')
$allGood = $true

foreach ($table in $tables) {
    $count = docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM brahmand.$table"
    $engine = docker exec clickhouse clickhouse-client --query "SELECT engine FROM system.tables WHERE database='brahmand' AND name='$table'"
    
    if ($engine -like "*MergeTree*") {
        Write-Host "✅ $table : $count rows, engine=$engine" -ForegroundColor Green
    } else {
        Write-Host "❌ $table : Wrong engine=$engine" -ForegroundColor Red
        $allGood = $false
    }
}

if (-not $allGood) {
    Write-Host "`n❌ Some benchmark tables failed verification!" -ForegroundColor Red
    exit 1
}

# Success summary
Write-Host "`n╔════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║  ✅ Windows MergeTree Fix Validated!         ║" -ForegroundColor Green
Write-Host "╠════════════════════════════════════════════════╣" -ForegroundColor Green
Write-Host "║  ✓ MergeTree tables create successfully       ║" -ForegroundColor Green
Write-Host "║  ✓ Data inserts work correctly                ║" -ForegroundColor Green
Write-Host "║  ✓ Data persists after restart                ║" -ForegroundColor Green
Write-Host "║  ✓ Benchmark data generation works            ║" -ForegroundColor Green
Write-Host "║  ✓ All benchmark tables verified              ║" -ForegroundColor Green
Write-Host "╚════════════════════════════════════════════════╝" -ForegroundColor Green

Write-Host "`n🎉 You can now run large-scale benchmarks with MergeTree!" -ForegroundColor Cyan
Write-Host "   Example: python tests/python/setup_benchmark_unified.py --scale 1000 --engine MergeTree" -ForegroundColor Gray

Write-Host "`n📊 Volume information:" -ForegroundColor Yellow
docker volume inspect clickhouse_data --format '{{.Mountpoint}}'
