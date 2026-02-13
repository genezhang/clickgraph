#!/bin/bash
# ClickGraph Server Background Launcher for Linux
# This script starts the ClickGraph server as a background process with full environment variable configuration
#
# NEW: Defaults to unified test schema for multi-schema support!
#
# Usage Examples:
#   ./start_server_background.sh                                    # Unified schema (recommended)
#   ./start_server_background.sh -c "ecommerce_graph_demo.yaml"     # Ecommerce schema (single)
#   ./start_server_background.sh -p 8081 -l "debug"                 # Custom port and logging
#   ./start_server_background.sh -d "test_db" --disable-bolt        # Custom database, HTTP only
#   ./start_server_background.sh --max-cte-depth 200 --validate     # Custom CTE depth with validation
#   ./start_server_background.sh --http-host "127.0.0.1"            # Secure binding

set -e

# Default values
HTTP_PORT=8080
BOLT_PORT=7687
CONFIG_PATH="schemas/test/unified_test_multi_schema.yaml"  # Multi-schema config with 6 isolated schemas
DATABASE="brahmand"  # Changed: Most tests use brahmand database
CLICKHOUSE_URL="http://localhost:8123"
CLICKHOUSE_USER="test_user"
CLICKHOUSE_PASSWORD="test_pass"
LOG_LEVEL="warn"  # Default to warn to reduce log volume (prevents terminal overflow)
HTTP_HOST="0.0.0.0"
BOLT_HOST="0.0.0.0"
DISABLE_BOLT=false
MAX_CTE_DEPTH=100
VALIDATE_SCHEMA=false
DEBUG_BUILD=false
NEO4J_COMPAT_MODE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--http-port)
            HTTP_PORT="$2"
            shift 2
            ;;
        -b|--bolt-port)
            BOLT_PORT="$2"
            shift 2
            ;;
        -c|--config)
            CONFIG_PATH="$2"
            shift 2
            ;;
        -d|--database)
            DATABASE="$2"
            shift 2
            ;;
        --clickhouse-url)
            CLICKHOUSE_URL="$2"
            shift 2
            ;;
        --clickhouse-user)
            CLICKHOUSE_USER="$2"
            shift 2
            ;;
        --clickhouse-password)
            CLICKHOUSE_PASSWORD="$2"
            shift 2
            ;;
        -l|--log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        --http-host)
            HTTP_HOST="$2"
            shift 2
            ;;
        --bolt-host)
            BOLT_HOST="$2"
            shift 2
            ;;
        --disable-bolt)
            DISABLE_BOLT=true
            shift
            ;;
        --max-cte-depth)
            MAX_CTE_DEPTH="$2"
            shift 2
            ;;
        --validate)
            VALIDATE_SCHEMA=true
            shift
            ;;
        --debug)
            DEBUG_BUILD=true
            shift
            ;;
        --neo4j-compat-mode)
            NEO4J_COMPAT_MODE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  -p, --http-port PORT          HTTP port (default: 8080)"
            echo "  -b, --bolt-port PORT          Bolt port (default: 7687)"
            echo "  -c, --config PATH             Config file path (default: social_network.yaml)"
            echo "  -d, --database NAME           Database name (default: social)"
            echo "  --clickhouse-url URL          ClickHouse URL (default: http://localhost:8123)"
            echo "  --clickhouse-user USER        ClickHouse user (default: test_user)"
            echo "  --clickhouse-password PASS    ClickHouse password (default: test_pass)"
            echo "  -l, --log-level LEVEL         Log level (default: info)"
            echo "  --http-host HOST              HTTP host (default: 0.0.0.0)"
            echo "  --bolt-host HOST              Bolt host (default: 0.0.0.0)"
            echo "  --disable-bolt                Disable Bolt protocol"
            echo "  --max-cte-depth DEPTH         Max CTE depth (default: 100)"
            echo "  --validate                    Enable schema validation"
            echo "  --debug                       Build in debug mode"
            echo "  -h, --help                    Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# Set environment variables
export CLICKHOUSE_URL="$CLICKHOUSE_URL"
export CLICKHOUSE_USER="$CLICKHOUSE_USER"
export CLICKHOUSE_PASSWORD="$CLICKHOUSE_PASSWORD"
export CLICKHOUSE_DATABASE="$DATABASE"
export GRAPH_CONFIG_PATH="$CONFIG_PATH"
export RUST_LOG="$LOG_LEVEL"
export CLICKGRAPH_HOST="$HTTP_HOST"
export CLICKGRAPH_PORT="$HTTP_PORT"
export CLICKGRAPH_BOLT_HOST="$BOLT_HOST"
export CLICKGRAPH_BOLT_PORT="$BOLT_PORT"
export CLICKGRAPH_BOLT_ENABLED=$( [ "$DISABLE_BOLT" = false ] && echo "true" || echo "false" )
export CLICKGRAPH_MAX_CTE_DEPTH="$MAX_CTE_DEPTH"
export CLICKGRAPH_VALIDATE_SCHEMA=$( [ "$VALIDATE_SCHEMA" = true ] && echo "true" || echo "false" )
export CLICKGRAPH_NEO4J_COMPAT_MODE=$( [ "$NEO4J_COMPAT_MODE" = true ] && echo "true" || echo "false" )

echo -e "\033[0;32mStarting ClickGraph server in background...\033[0m"
echo -e "\033[0;36mHTTP Port: $HTTP_PORT (Host: $HTTP_HOST)\033[0m"
echo -e "\033[0;36mBolt Port: $BOLT_PORT (Host: $BOLT_HOST, Enabled: $CLICKGRAPH_BOLT_ENABLED)\033[0m"
echo -e "\033[0;36mConfig: $CONFIG_PATH\033[0m"
echo -e "\033[0;36mDatabase: $DATABASE\033[0m"
echo -e "\033[0;36mClickHouse: $CLICKHOUSE_URL\033[0m"
echo -e "\033[0;36mLog Level: $LOG_LEVEL\033[0m"
echo -e "\033[0;36mMax CTE Depth: $MAX_CTE_DEPTH\033[0m"
echo -e "\033[0;36mNeo4j Compat: $NEO4J_COMPAT_MODE\033[0m"

# Determine build mode
if [ "$DEBUG_BUILD" = true ]; then
    BUILD_MODE="debug"
    CARGO_FLAGS=""
    echo -e "\033[0;33mBuild Mode: Debug\033[0m"
else
    BUILD_MODE="release"
    CARGO_FLAGS="--release"
    echo -e "\033[0;33mBuild Mode: Release\033[0m"
fi

# Build the project
echo -e "\033[0;33mBuilding ClickGraph...\033[0m"
cargo build $CARGO_FLAGS --bin clickgraph

# Build CLI arguments
CLI_ARGS="--http-port $HTTP_PORT --bolt-port $BOLT_PORT --http-host $HTTP_HOST --bolt-host $BOLT_HOST --max-cte-depth $MAX_CTE_DEPTH"

if [ "$DISABLE_BOLT" = true ]; then
    CLI_ARGS="$CLI_ARGS --disable-bolt"
fi

if [ "$VALIDATE_SCHEMA" = true ]; then
    CLI_ARGS="$CLI_ARGS --validate-schema"
fi

if [ "$NEO4J_COMPAT_MODE" = true ]; then
    CLI_ARGS="$CLI_ARGS --neo4j-compat-mode"
fi

# Start server in background
LOG_FILE="clickgraph_server.log"
PID_FILE="clickgraph_server.pid"

echo -e "\033[0;33mStarting server (logs: $LOG_FILE)...\033[0m"
nohup ./target/$BUILD_MODE/clickgraph $CLI_ARGS > "$LOG_FILE" 2>&1 &
SERVER_PID=$!
echo $SERVER_PID > "$PID_FILE"

# Wait a moment and check if it's still running
sleep 2
if kill -0 $SERVER_PID 2>/dev/null; then
    echo -e "\033[0;32m✓ Server started successfully (PID: $SERVER_PID)\033[0m"
    echo -e "\033[0;36mView logs: tail -f $LOG_FILE\033[0m"
    echo -e "\033[0;36mStop server: kill \$(cat $PID_FILE) && rm $PID_FILE\033[0m"
    echo -e "\033[0;36mOr: pkill -f clickgraph\033[0m"
else
    echo -e "\033[0;31m✗ Server failed to start. Check $LOG_FILE for details.\033[0m"
    cat "$LOG_FILE"
    rm -f "$PID_FILE"
    exit 1
fi
