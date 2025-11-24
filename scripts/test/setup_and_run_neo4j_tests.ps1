# Quick Neo4j Testing Setup for Windows
# Run this script to setup and execute Neo4j semantics tests

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Neo4j Semantics Testing - Quick Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Step 1: Check Docker
Write-Host "`n[1/5] Checking Docker..." -ForegroundColor Yellow
try {
    docker version | Out-Null
    Write-Host "[OK] Docker is installed" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Docker not found. Please install Docker Desktop." -ForegroundColor Red
    Write-Host "   https://www.docker.com/products/docker-desktop" -ForegroundColor Yellow
    exit 1
}

# Step 2: Start Neo4j
Write-Host "`n[2/5] Starting Neo4j container..." -ForegroundColor Yellow

# Stop and remove existing container if it exists
docker stop neo4j-test 2>$null | Out-Null
docker rm neo4j-test 2>$null | Out-Null

# Start new container
docker run -d `
    --name neo4j-test `
    -p 7474:7474 -p 7687:7687 `
    -e NEO4J_AUTH=neo4j/testpassword `
    neo4j:latest | Out-Null

if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] Neo4j container started" -ForegroundColor Green
} else {
    Write-Host "[ERROR] Failed to start Neo4j" -ForegroundColor Red
    exit 1
}

# Wait for Neo4j to be ready
Write-Host "   Waiting for Neo4j to start (30 seconds)..." -ForegroundColor Gray
Start-Sleep -Seconds 30

# Check if Neo4j is ready
Write-Host "   Checking Neo4j status..." -ForegroundColor Gray
$logs = docker logs neo4j-test 2>&1 | Select-String "Started"
if ($logs) {
    Write-Host "[OK] Neo4j is ready" -ForegroundColor Green
} else {
    Write-Host "[WARN] Neo4j may still be starting. Test will retry connection." -ForegroundColor Yellow
}

# Step 3: Install Python dependencies
Write-Host "`n[3/5] Installing Python dependencies..." -ForegroundColor Yellow
try {
    pip install neo4j --quiet
    Write-Host "[OK] Python neo4j driver installed" -ForegroundColor Green
} catch {
    Write-Host "[WARN] pip install may have had issues, but continuing..." -ForegroundColor Yellow
}

# Step 4: Display access info
Write-Host "`n[4/5] Neo4j Access Information:" -ForegroundColor Yellow
Write-Host "   Browser: http://localhost:7474" -ForegroundColor Cyan
Write-Host "   Bolt:    bolt://localhost:7687" -ForegroundColor Cyan
Write-Host "   User:    neo4j" -ForegroundColor Cyan
Write-Host "   Pass:    testpassword" -ForegroundColor Cyan

# Step 5: Run tests
Write-Host "`n[5/5] Running Neo4j semantics tests..." -ForegroundColor Yellow
Write-Host "========================================`n" -ForegroundColor Cyan

python scripts\test\neo4j_semantics_verification.py

# Cleanup prompt
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Testing Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan

$cleanup = Read-Host "`nDo you want to stop and remove the Neo4j container? (y/N)"
if ($cleanup -eq "y" -or $cleanup -eq "Y") {
    Write-Host "`nCleaning up..." -ForegroundColor Yellow
    docker stop neo4j-test | Out-Null
    docker rm neo4j-test | Out-Null
    Write-Host "[OK] Neo4j container removed" -ForegroundColor Green
} else {
    Write-Host "`n[INFO] Neo4j container is still running." -ForegroundColor Cyan
    Write-Host "   To stop it later: docker stop neo4j-test" -ForegroundColor Gray
    Write-Host "   To remove it: docker rm neo4j-test" -ForegroundColor Gray
}

Write-Host "`n[DONE] Testing complete!" -ForegroundColor Green
