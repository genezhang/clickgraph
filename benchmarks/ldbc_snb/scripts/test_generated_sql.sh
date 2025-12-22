#!/bin/bash
# Test Generated SQL in ClickHouse
# This script runs captured SQL files directly against the LDBC ClickHouse instance

CLICKHOUSE_HOST="localhost"
CLICKHOUSE_PORT="18123"
CLICKHOUSE_USER="test_user"
CLICKHOUSE_PASSWORD="test_pass"
CLICKHOUSE_DB="ldbc"

SQL_DIR="$(dirname "$0")/../results/generated_sql"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "================================================================================"
echo "  Testing Generated SQL in ClickHouse"
echo "================================================================================"
echo "  Host:     $CLICKHOUSE_HOST:$CLICKHOUSE_PORT"
echo "  Database: $CLICKHOUSE_DB"
echo "  SQL Dir:  $SQL_DIR"
echo ""

# Function to execute SQL and check result
test_sql_file() {
    local sql_file="$1"
    local query_name=$(basename "$sql_file" .sql)
    
    # Extract just the SQL (skip comment lines)
    local sql=$(grep -v '^--' "$sql_file" | grep -v '^$')
    
    # Replace parameters with sample values
    sql=$(echo "$sql" | sed \
        -e 's/\$personId/933/g' \
        -e 's/\$person1Id/933/g' \
        -e 's/\$person2Id/8796093022390/g' \
        -e 's/\$firstName/'\''Chau'\''/g' \
        -e 's/\$countryName/'\''India'\''/g' \
        -e 's/\$country/'\''India'\''/g' \
        -e 's/\$tagClassName/'\''MusicalArtist'\''/g' \
        -e 's/\$tagName/'\''Arnold_Schwarzenegger'\''/g')
    
    # Execute SQL
    result=$(curl -s "http://$CLICKHOUSE_HOST:$CLICKHOUSE_PORT/" \
        --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
        --data-binary "$sql" \
        -H "X-ClickHouse-Database: $CLICKHOUSE_DB" \
        -H "X-ClickHouse-Format: JSONCompact" 2>&1)
    
    # Check for errors
    if echo "$result" | grep -q "Code:"; then
        echo -e "  ${query_name:0:30} ${RED}✗ FAIL${NC}"
        echo "      Error: $(echo "$result" | head -1)"
        return 1
    else
        # Count rows
        row_count=$(echo "$result" | jq '.rows' 2>/dev/null || echo "0")
        echo -e "  ${query_name:0:30} ${GREEN}✓ PASS${NC}  ($row_count rows)"
        return 0
    fi
}

# Test all SQL files
passed=0
failed=0

echo "Testing SQL files..."
echo ""

for sql_file in "$SQL_DIR"/*.sql; do
    if [ -f "$sql_file" ] && [ $(basename "$sql_file") != "audit_report.md" ]; then
        if test_sql_file "$sql_file"; then
            ((passed++))
        else
            ((failed++))
        fi
    fi
done

echo ""
echo "================================================================================"
echo "  SUMMARY"
echo "================================================================================"
echo "  Total:  $((passed + failed))"
echo -e "  ${GREEN}✓ Pass: $passed${NC}"
echo -e "  ${RED}✗ Fail: $failed${NC}"
echo "  Rate:   $((100 * passed / (passed + failed)))%"
echo "================================================================================"
echo ""

if [ $failed -eq 0 ]; then
    exit 0
else
    exit 1
fi
