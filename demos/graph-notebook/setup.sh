#!/bin/bash
# Setup script for ClickGraph + Graph-Notebook Demo
# Starts all services and loads demo data

set -e

echo "=========================================="
echo "ClickGraph + Graph-Notebook Demo Setup"
echo "=========================================="
echo ""

# Start services
echo "📦 Starting services with docker-compose..."
echo ""
docker-compose up -d

# Wait for services
echo "⏳ Waiting for services to be ready (30 seconds)..."
sleep 30

echo ""
echo "✓ All services started!"
echo ""

# Load demo data
echo "📊 Loading demo data..."
echo ""
cd ../neo4j-browser
bash setup_demo_data.sh
cd ../graph-notebook

echo ""
echo "=========================================="
echo "✅ Setup Complete!"
echo "=========================================="
echo ""
echo "Access Jupyter Notebook:"
echo "  🌐 http://localhost:8888"
echo ""
echo "Open the demo notebook:"
echo "  📓 work/clickgraph-demo.ipynb"
echo ""
echo "ClickGraph endpoints:"
echo "  HTTP API: http://localhost:7475"
echo "  Bolt Protocol: bolt://localhost:7687"
echo ""
echo "Sample dataset loaded:"
echo "  • 30 users"
echo "  • 50 posts"
echo "  • 60 FOLLOWS relationships"
echo "  • 50 AUTHORED relationships"
echo "  • 80 LIKED relationships"
echo ""
echo "=========================================="
echo "First Query to Try:"
echo "=========================================="
echo ""
echo "%%oc"
echo "MATCH (u:User)-[:FOLLOWS]->(f)"
echo "RETURN u, f"
echo "LIMIT 10"
echo ""
echo "=========================================="
echo "Manage Services:"
echo "=========================================="
echo ""
echo "Stop all services:"
echo "  cd demos/graph-notebook && docker-compose down"
echo ""
echo "View logs:"
echo "  docker logs clickhouse-notebook-demo"
echo "  docker logs clickgraph-notebook-demo"
echo "  docker logs jupyter-notebook-demo"
echo ""
echo "Clean up volumes:"
echo "  docker-compose down -v"
echo ""
