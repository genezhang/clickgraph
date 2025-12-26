#!/bin/bash
# Pre-commit hook to prevent denormalized schema regressions
#
# Install: ln -s ../../scripts/hooks/pre-commit.sh .git/hooks/pre-commit
#
# This hook ensures that changes to VLP/relationship code are tested
# across ALL schema types before committing.

set -e

CHANGED_FILES=$(git diff --cached --name-only)

# Files that require multi-schema testing
CRITICAL_FILES=(
    "src/render_plan/cte_extraction.rs"
    "src/render_plan/plan_builder.rs"
    "src/clickhouse_query_generator/variable_length_cte.rs"
    "src/query_planner/analyzer/graph_traversal_planning.rs"
)

# Check if any critical files changed
CRITICAL_CHANGED=false
for file in "${CRITICAL_FILES[@]}"; do
    if echo "$CHANGED_FILES" | grep -q "$file"; then
        CRITICAL_CHANGED=true
        echo "⚠️  Critical file changed: $file"
    fi
done

if [ "$CRITICAL_CHANGED" = true ]; then
    echo ""
    echo "🧪 Critical VLP/relationship code changed!"
    echo "   Running multi-schema validation tests..."
    echo ""
    
    # Run meta tests to ensure coverage
    if ! pytest tests/meta/test_schema_coverage.py -v; then
        echo ""
        echo "❌ Schema coverage tests FAILED!"
        echo ""
        echo "Your changes may break denormalized schemas."
        echo "Please ensure:"
        echo "  1. Tests exist for BOTH traditional AND denormalized schemas"
        echo "  2. No xfail markers on critical features"
        echo "  3. Code comments explain multi-schema handling"
        echo ""
        echo "See: docs/development/schema-testing-requirements.md"
        echo ""
        echo "To bypass this check (NOT recommended):"
        echo "  git commit --no-verify"
        exit 1
    fi
    
    # Run denormalized VLP tests
    echo ""
    echo "Running denormalized VLP tests..."
    if ! pytest tests/integration/test_denormalized_edges.py::TestDenormalizedVariableLengthPaths -v -x; then
        echo ""
        echo "❌ Denormalized VLP tests FAILED!"
        echo ""
        echo "This feature has broken multiple times. Please:"
        echo "  1. Fix the failing tests OR"
        echo "  2. Revert your changes OR"
        echo "  3. Mark PR as draft and create issue"
        echo ""
        echo "NEVER commit xfail on VLP tests!"
        exit 1
    fi
    
    echo ""
    echo "✅ Multi-schema validation passed!"
    echo ""
fi

exit 0
