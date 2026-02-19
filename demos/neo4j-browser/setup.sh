#!/bin/bash
# ClickGraph + Neo4j Browser Quick Start Setup
# End-user setup: simple, self-contained demo with docker-compose
# 
# Usage: bash setup.sh
# This will start all services and open Neo4j Browser automatically

set -e

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
DEMO_DIR="$REPO_ROOT/demos/neo4j-browser"

echo "=========================================="
echo "ClickGraph + Neo4j Browser Quick Start"
echo "=========================================="
echo ""

# Check if docker is available
if ! command -v docker &> /dev/null; then
    echo "❌ Docker not found. Please install Docker first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose not found. Please install Docker Compose first."
    exit 1
fi

echo "📦 Starting services with docker-compose..."
echo ""

cd "$DEMO_DIR"

# Start services
docker-compose up -d

echo "✓ Services started!"
echo ""

# Wait for services to be healthy
echo "⏳ Waiting for services to be ready (30 seconds)..."
sleep 30

echo ""
echo "✓ All services ready!"
echo ""

# Health checks
echo "📋 Service Status:"
echo ""

if curl -s http://localhost:8123/ping > /dev/null 2>&1; then
    echo "✓ ClickHouse: Ready (http://localhost:8123)"
else
    echo "⚠️  ClickHouse: Still starting (check logs: docker logs clickhouse-demo)"
fi

if curl -s http://localhost:7474 > /dev/null 2>&1; then
    echo "✓ Neo4j Browser: Ready (http://localhost:7474)"
else
    echo "⚠️  Neo4j Browser: Still starting (check logs: docker logs neo4j-demo)"
fi

if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "✓ ClickGraph: Ready (Bolt: bolt://localhost:7687)"
else
    echo "⚠️  ClickGraph: Still starting (check logs: docker logs clickgraph-demo)"
fi

echo ""
echo "=========================================="
echo "🎉 Quick Start Complete!"
echo "=========================================="
echo ""

# Open browser if possible
if command -v xdg-open &> /dev/null; then
    xdg-open "http://localhost:7474" 2>/dev/null &
elif command -v open &> /dev/null; then
    open "http://localhost:7474" 2>/dev/null &
fi

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

echo "2. Connect to ClickGraph:"
echo "   - Click 'Database' dropdown (top right)"
echo "   - Click 'Connect to another database'"
echo "   - URI: bolt://localhost:7687"
echo "   - Leave username/password empty"
echo "   - Click 'Connect'"
echo ""

echo "3. Try a simple query:"
echo "   MATCH (u:User) RETURN u LIMIT 5"
echo ""

echo "=========================================="
echo "Manage Services:"
echo "=========================================="
echo ""

echo "Stop all services:"
echo "  cd $DEMO_DIR && docker-compose down"
echo ""

echo "View logs:"
echo "  docker logs clickhouse-demo    # ClickHouse logs"
echo "  docker logs neo4j-demo         # Neo4j logs"
echo "  docker logs clickgraph-demo    # ClickGraph logs"
echo ""

echo "Clean up volumes (warning: deletes data):"
echo "  cd $DEMO_DIR && docker-compose down -v"
echo ""

echo "=========================================="
echo "Troubleshooting:"
echo "=========================================="
echo ""

echo "Services not starting? Check logs:"
echo "  docker-compose logs -f"
echo ""

echo "Connection refused to ClickGraph?"
echo "  Check if port 7687 is in use: netstat -tlnp | grep 7687"
echo ""

echo "Neo4j Browser not responding?"
echo "  Try: curl http://localhost:7474"
echo "  Wait another 30 seconds for startup"
echo ""
