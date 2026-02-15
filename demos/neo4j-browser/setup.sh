#!/bin/bash
# ClickGraph + Neo4j Browser Setup Script
# One-command setup to launch Neo4j Browser with ClickGraph Bolt connection
# Uses simple docker run for Neo4j (more reliable than docker-compose)

REPO_ROOT="/home/gz/clickgraph"

echo "=========================================="
echo "ClickGraph + Neo4j Browser Demo Setup"
echo "=========================================="
echo ""

# Check if docker is available
if ! command -v docker &> /dev/null; then
    echo "❌ Docker not found. Please install Docker first."
    exit 1
fi

echo "📦 Step 1: Starting services..."
echo ""

cd "$REPO_ROOT"

# Check what's running
NEO4J_SIMPLE=$(docker ps --format "table {{.Names}}" | grep -c "neo4j-clickgraph" || true)
NEO4J_DEV=$(docker ps --format "table {{.Names}}" | grep -c "neo4j-dev" || true)
CLICKGRAPH_RUNNING=$(docker ps --format "table {{.Names}}" | grep -c "clickgraph-dev" || true)

# Clean up broken neo4j-dev container if it exists
if docker ps -a --format "table {{.Names}}" | grep -q "neo4j-dev"; then
    echo "→ Cleaning up Neo4j (docker-compose version)..."
    docker rm -f neo4j-dev 2>/dev/null || true
    sleep 2
fi

# Start Neo4j standalone (simpler, more reliable)
if [ "$NEO4J_SIMPLE" -eq 0 ]; then
    echo "→ Starting Neo4j Browser..."
    docker run --rm -d \
      --name neo4j-clickgraph \
      -p 7474:7474 \
      -p 7687:7687 \
      -e NEO4J_AUTH=neo4j/test_password \
      neo4j:latest 2>/dev/null
    
    echo "  Waiting for Neo4j to start (30 seconds)..."
    sleep 30
else
    echo "✓ Neo4j already running (neo4j-clickgraph)"
fi

echo ""

# Check ClickGraph
if [ "$CLICKGRAPH_RUNNING" -eq 0 ]; then
    echo "→ Starting ClickGraph..."
    docker-compose -f docker-compose.dev.yaml up -d clickgraph
    echo "  Waiting for ClickGraph to start (10 seconds)..."
    sleep 10
else
    echo "✓ ClickGraph already running"
fi

echo ""
echo "✓ All services started!"
echo ""

# Health checks
echo "📋 Step 2: Verifying services..."
echo ""

# Check Neo4j
if curl -s http://localhost:7474 > /dev/null 2>&1; then
    echo "✓ Neo4j Browser: http://localhost:7474"
else
    echo "⚠️  Neo4j Browser: Not responding yet (might still be starting)"
fi

# Check ClickGraph
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "✓ ClickGraph HTTP: http://localhost:8080"
    echo "✓ ClickGraph Bolt: bolt://localhost:7687"
else
    echo "⚠️  ClickGraph: Not responding (check logs: docker logs clickgraph-dev)"
fi

echo ""
echo "=========================================="
echo "🎉 Ready to use!"
echo "=========================================="
echo ""

# Open browser if possible
echo "Opening Neo4j Browser..."
sleep 2

if command -v xdg-open &> /dev/null; then
    xdg-open "http://localhost:7474" 2>/dev/null &
elif command -v open &> /dev/null; then
    open "http://localhost:7474" 2>/dev/null &
fi

echo ""
echo "👉 Open in your browser: http://localhost:7474"
echo ""

echo "=========================================="
echo "Next Steps:"
echo "=========================================="
echo ""
echo "1. Login to Neo4j Browser:"
echo "   Username: neo4j"
echo "   Password: test_password"
echo ""
echo "2. In the Browser, click 'Database' dropdown"
echo "   Then: Connect to another database"
echo "   URI: bolt://localhost:7687"
echo "   (Leave username/password empty)"
echo ""
echo "3. Try a query:"
echo "   MATCH (u:User {user_id: 1}) RETURN u LIMIT 1"
echo ""
echo "4. More queries: demos/neo4j-browser/SAMPLE_QUERIES.md"
echo ""
echo "📊 Manage services:"
echo "   Stop Neo4j:    docker stop neo4j-clickgraph"
echo "   Stop ClickGraph: docker-compose -f docker-compose.dev.yaml down"
echo "   View logs:     docker logs neo4j-clickgraph"
echo ""
echo "💡 For troubleshooting, see:"
echo "   demos/neo4j-browser/CONNECTION_GUIDE.md"
echo ""
