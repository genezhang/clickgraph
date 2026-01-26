#!/bin/bash
# Test runner script for WITH CTE node expansion tests
# Usage: ./scripts/test/run_with_cte_tests.sh [options]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TESTS_DIR="$PROJECT_ROOT/tests/integration"
TEST_FILE="test_with_cte_node_expansion.py"

CLICKGRAPH_URL="${CLICKGRAPH_URL:-http://localhost:8080}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-test_user}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-test_pass}"

# Defaults
VERBOSE=false
SPECIFIC_TEST=""
SHOW_SQL=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -t|--test)
            SPECIFIC_TEST="$2"
            shift 2
            ;;
        -s|--show-sql)
            SHOW_SQL=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

show_help() {
    cat << EOF
WITH CTE Node Expansion Tests - Test Runner

Usage: $0 [options]

Options:
    -v, --verbose           Show verbose pytest output
    -t, --test TEST_NAME    Run specific test (e.g., test_with_single_node_export)
    -s, --show-sql          Show SQL generation (enable sql_only mode)
    -h, --help              Show this help message

Environment Variables:
    CLICKGRAPH_URL          ClickGraph server URL (default: http://localhost:8080)
    CLICKHOUSE_URL          ClickHouse URL (default: http://localhost:8123)
    CLICKHOUSE_USER         ClickHouse user (default: test_user)
    CLICKHOUSE_PASSWORD     ClickHouse password (default: test_pass)

Test Categories:
    Basic Expansion Tests:
        - TestWithBasicNodeExpansion
        - TestWithMultipleVariableExport
    
    Nested/Chaining Tests:
        - TestWithChaining
    
    Scalar/Aggregate Tests:
        - TestWithScalarExport
    
    Rename/Alias Tests:
        - TestWithPropertyRename
    
    Complex Pattern Tests:
        - TestWithCrossTable
        - TestWithOptionalMatch
    
    Edge Case Tests:
        - TestWithPolymorphicLabels
        - TestWithDenormalizedEdges
    
    Regression Tests:
        - TestWithRegressionCases

Examples:
    # Run all tests
    $0
    
    # Run specific test class
    $0 -t TestWithBasicNodeExpansion
    
    # Run specific test
    $0 -t test_with_single_node_export
    
    # Verbose output
    $0 -v
    
    # Show SQL generation
    $0 -s

EOF
}

check_servers() {
    echo -e "${BLUE}📋 Checking server availability...${NC}"
    
    # Check ClickGraph
    echo -n "  ClickGraph ($CLICKGRAPH_URL): "
    if curl -s "$CLICKGRAPH_URL/health" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Running${NC}"
    else
        echo -e "${RED}✗ Not responding${NC}"
        return 1
    fi
    
    # Check ClickHouse
    echo -n "  ClickHouse ($CLICKHOUSE_URL): "
    if curl -s "$CLICKHOUSE_URL/ping" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Running${NC}"
    else
        echo -e "${RED}✗ Not responding${NC}"
        return 1
    fi
    
    return 0
}

build_pytest_args() {
    local args=("$TEST_FILE")
    
    if [[ "$VERBOSE" == true ]]; then
        args+=("-v" "--tb=short")
    else
        args+=("-q")
    fi
    
    if [[ ! -z "$SPECIFIC_TEST" ]]; then
        args+=("-k" "$SPECIFIC_TEST")
    fi
    
    if [[ "$SHOW_SQL" == true ]]; then
        args+=("-s")  # Show stdout/stderr
    fi
    
    echo "${args[@]}"
}

main() {
    echo -e "${BLUE}"
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║   WITH CTE Node Expansion Test Suite                         ║"
    echo "║   Testing: Basic, Multi-var, Chaining, Scalars, Renames      ║"
    echo "║   Plus edge cases: Polymorphic, Denormalized                 ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
    
    # Check servers
    if ! check_servers; then
        echo -e "${RED}✗ Required servers not available${NC}"
        echo "  Start ClickHouse: docker-compose up -d"
        echo "  Start ClickGraph: cargo run --bin clickgraph"
        exit 1
    fi
    
    echo ""
    echo -e "${BLUE}🧪 Running tests...${NC}"
    echo "  Test file: $TEST_FILE"
    echo "  Project root: $PROJECT_ROOT"
    echo ""
    
    # Set environment
    export CLICKGRAPH_URL
    export CLICKHOUSE_URL
    export CLICKHOUSE_USER
    export CLICKHOUSE_PASSWORD
    
    # Build pytest args
    local pytest_args=$(build_pytest_args)
    
    # Run pytest
    cd "$TESTS_DIR"
    
    if pytest $pytest_args; then
        echo ""
        echo -e "${GREEN}✓ All tests passed!${NC}"
        echo ""
        echo "Test Categories Verified:"
        echo "  ✓ Basic node expansion"
        echo "  ✓ Multi-variable exports"
        echo "  ✓ WITH chaining (nested CTEs)"
        echo "  ✓ Scalar aggregates (no expansion)"
        echo "  ✓ Property renames"
        echo "  ✓ Cross-table patterns"
        echo "  ✓ Optional match + WITH"
        echo "  ✓ Polymorphic labels (edge case)"
        echo "  ✓ Denormalized edges (edge case)"
        echo "  ✓ Regression: Base table expansion"
        exit 0
    else
        echo ""
        echo -e "${RED}✗ Some tests failed${NC}"
        exit 1
    fi
}

main
