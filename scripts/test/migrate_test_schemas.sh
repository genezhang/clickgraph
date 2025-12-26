#!/bin/bash
# Migration script to update all remaining test files from old unified_test_schema 
# to new multi-schema format (unified_test_multi_schema.yaml)

set -e

echo "=== Test Schema Migration Script ==="
echo "Migrating from unified_test_schema.yaml to unified_test_multi_schema.yaml"
echo ""

cd /home/gene/clickgraph

# Find all test files that still reference unified_test_schema
echo "Finding files with 'unified_test_schema' references..."
files=$(grep -l "unified_test_schema" tests/integration/*.py 2>/dev/null || true)

if [ -z "$files" ]; then
    echo "✓ No files found with unified_test_schema references"
    exit 0
fi

echo "Files to update:"
echo "$files" | sed 's/^/  - /'
echo ""

# Count total references
total=$(grep -o "unified_test_schema" tests/integration/*.py 2>/dev/null | wc -l)
echo "Total references found: $total"
echo ""

# Create backup
echo "Creating backup..."
backup_dir="tests/integration/.backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$backup_dir"
for file in $files; do
    cp "$file" "$backup_dir/"
done
echo "✓ Backup created in $backup_dir"
echo ""

# Perform replacements
echo "Performing replacements..."

# Strategy: Replace unified_test_schema with appropriate schema based on labels used:
# - Files using TestUser/TestProduct → test_fixtures
# - Files using User/Post (brahmand) → social_benchmark  
# - Files using Person → ldbc_snb
# - Default → test_fixtures

for file in $files; do
    echo "Processing $file..."
    
    # Check what labels the file uses
    if grep -q "TestUser\|TestProduct\|TestGroup" "$file"; then
        schema="test_fixtures"
    elif grep -q ":Person\|:Comment\|:Forum" "$file"; then
        schema="ldbc_snb"
    elif grep -q ":User.*brahmand\|users_bench" "$file"; then
        schema="social_benchmark"
    elif grep -q ":Airport\|:Flight.*travel" "$file"; then
        schema="denormalized_flights"
    else
        # Default to test_fixtures for generic test files
        schema="test_fixtures"
    fi
    
    # Replace all occurrences
    sed -i "s/unified_test_schema/$schema/g" "$file"
    
    count=$(grep -c "$schema" "$file" || echo "0")
    echo "  → Replaced with '$schema' ($count occurrences)"
done

echo ""
echo "=== Migration Complete ==="
echo ""
echo "Summary:"
echo "  Files updated: $(echo "$files" | wc -l)"
echo "  Backup location: $backup_dir"
echo ""
echo "Next steps:"
echo "  1. Review changes: git diff tests/integration/"
echo "  2. Run tests: pytest tests/integration/ -v"
echo "  3. If successful: git add tests/integration/ && git commit"
echo "  4. If issues: restore from $backup_dir"
