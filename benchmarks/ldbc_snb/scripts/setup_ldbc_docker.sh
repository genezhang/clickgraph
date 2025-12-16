#!/bin/bash
# LDBC Data Loading Helper for ClickHouse Docker Container
# Run this after starting your clickhouse-ldbc container

set -e

echo "=========================================="
echo "LDBC SNB Data Loading Script"
echo "=========================================="
echo ""

# Configuration
CONTAINER_NAME="clickhouse-ldbc"
DB_NAME="ldbc"
CH_USER="default"
CH_PASSWORD="default"
LDBC_DIR="/root/ldbc"

echo "Step 1: Check container is running..."
if ! docker ps | grep -q $CONTAINER_NAME; then
    echo "❌ Container $CONTAINER_NAME not running!"
    echo "Start it with: docker start $CONTAINER_NAME"
    exit 1
fi
echo "✅ Container running"

echo ""
echo "Step 2: Create database..."
docker exec -i $CONTAINER_NAME clickhouse-client --query "CREATE DATABASE IF NOT EXISTS $DB_NAME"
echo "✅ Database $DB_NAME created"

echo ""
echo "Step 3: Check mounted directory..."
docker exec -i $CONTAINER_NAME ls -la $LDBC_DIR/schemas/ > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ LDBC directory mounted correctly"
else
    echo "❌ LDBC directory not found in container!"
    echo "Expected: $LDBC_DIR"
    exit 1
fi

echo ""
echo "Step 4: Create tables..."
echo "Using schema: $LDBC_DIR/schemas/clickhouse_ddl.sql"
docker exec -i $CONTAINER_NAME clickhouse-client --database=$DB_NAME < benchmarks/ldbc_snb/schemas/clickhouse_ddl.sql
echo "✅ Tables created"

echo ""
echo "Step 5: Check for data files..."
DATA_DIR="ldbc/data"
if [ ! -d "$DATA_DIR" ]; then
    echo "⚠️  Data directory not found: $DATA_DIR"
    echo ""
    echo "To download LDBC data:"
    echo "  cd benchmarks/ldbc_snb"
    echo "  ./scripts/download_data.sh"
    echo ""
    read -p "Do you want to continue without data? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    echo "✅ Data directory found"
    
    echo ""
    echo "Step 6: Load data..."
    echo "This may take several minutes..."
    
    # Load each table (example - adjust based on actual data files)
    for table in person knows message forum post comment; do
        if [ -f "$DATA_DIR/${table}.csv" ]; then
            echo "Loading $table..."
            docker exec -i $CONTAINER_NAME clickhouse-client --database=$DB_NAME \
                --query="INSERT INTO ${table} FORMAT CSV" < "$DATA_DIR/${table}.csv"
        fi
    done
    
    echo "✅ Data loaded"
fi

echo ""
echo "Step 7: Verify setup..."
docker exec -i $CONTAINER_NAME clickhouse-client --database=$DB_NAME --query="
SELECT 
    database,
    name as table_name,
    total_rows
FROM system.tables
WHERE database = '$DB_NAME'
ORDER BY name
"

echo ""
echo "=========================================="
echo "✅ LDBC Setup Complete!"
echo "=========================================="
echo ""
echo "Connection details for ClickGraph:"
echo "  CLICKHOUSE_URL=http://localhost:18123"
echo "  CLICKHOUSE_DATABASE=$DB_NAME"
echo "  GRAPH_CONFIG_PATH=./benchmarks/ldbc_snb/schemas/ldbc_snb.yaml"
echo ""
echo "To start ClickGraph server:"
echo "  pkill -f clickgraph"
echo "  CLICKHOUSE_URL='http://localhost:18123' \\"
echo "  CLICKHOUSE_USER='default' \\"
echo "  CLICKHOUSE_PASSWORD='default' \\"
echo "  CLICKHOUSE_DATABASE='$DB_NAME' \\"
echo "  GRAPH_CONFIG_PATH='./benchmarks/ldbc_snb/schemas/ldbc_snb.yaml' \\"
echo "  RUST_LOG=info ./target/debug/clickgraph > /tmp/server.log 2>&1 &"
echo ""
