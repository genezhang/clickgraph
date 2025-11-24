# Quick Neo4j Test Runner
# Uses existing Neo4j container or starts a new one

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Neo4j Semantics Testing" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Check if Neo4j is already running
Write-Host "`nChecking for existing Neo4j container..." -ForegroundColor Yellow
$existing = docker ps --filter "name=neo4j" --format "{{.Names}}"

if ($existing) {
    Write-Host "[OK] Found running Neo4j container: $existing" -ForegroundColor Green
    Write-Host "   Using existing container for tests" -ForegroundColor Gray
} else {
    Write-Host "[INFO] No running Neo4j found, starting new container..." -ForegroundColor Yellow
    
    # Clean up any stopped neo4j-test containers
    docker rm neo4j-test 2>$null | Out-Null
    
    # Start new container
    docker run -d `
        --name neo4j-test `
        -p 7474:7474 -p 7687:7687 `
        -e NEO4J_AUTH=neo4j/testpassword `
        neo4j:latest | Out-Null
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] Neo4j container started" -ForegroundColor Green
        Write-Host "   Waiting 30 seconds for startup..." -ForegroundColor Gray
        Start-Sleep -Seconds 30
    } else {
        Write-Host "[ERROR] Failed to start Neo4j" -ForegroundColor Red
        Write-Host "   Port may be in use. Check: docker ps -a | Select-String neo4j" -ForegroundColor Yellow
        exit 1
    }
}

# Install Python dependency
Write-Host "`nChecking Python neo4j driver..." -ForegroundColor Yellow
try {
    python -c "import neo4j" 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] neo4j driver already installed" -ForegroundColor Green
    } else {
        Write-Host "[INFO] Installing neo4j driver..." -ForegroundColor Yellow
        pip install neo4j --quiet
        Write-Host "[OK] Installed" -ForegroundColor Green
    }
} catch {
    Write-Host "[WARN] Could not verify neo4j driver" -ForegroundColor Yellow
}

# Display connection info
Write-Host "`nNeo4j Connection:" -ForegroundColor Yellow
Write-Host "   Browser: http://localhost:7474" -ForegroundColor Cyan
Write-Host "   Bolt:    bolt://localhost:7687" -ForegroundColor Cyan
Write-Host "   User:    neo4j" -ForegroundColor Cyan
Write-Host "   Pass:    testpassword (or your existing password)" -ForegroundColor Cyan

# Run tests
Write-Host "`nRunning tests..." -ForegroundColor Yellow
Write-Host "========================================`n" -ForegroundColor Cyan

python scripts\test\neo4j_semantics_verification.py

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "[DONE] Testing complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
