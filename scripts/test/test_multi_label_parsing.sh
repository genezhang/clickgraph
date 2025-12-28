#!/bin/bash
# Test multi-label node parsing syntax
# Part 1B verification script

set -e

echo "=== Testing Multi-Label Node Parsing ==="
echo ""

# Single label (should work as before)
echo "Test 1: Single label node"
echo 'MATCH (x:User) RETURN x' | cargo run --bin clickgraph -- --sql-only 2>/dev/null && echo "✓ Single label works" || echo "✗ Single label failed"
echo ""

# Multi-label node (new syntax)
echo "Test 2: Multi-label node (x:User|Post)"
echo 'MATCH (x:User|Post) RETURN x' | cargo run --bin clickgraph -- --sql-only 2>/dev/null && echo "✓ Multi-label parsing works" || echo "✗ Multi-label parsing failed"
echo ""

# Multi-label in connected pattern
echo "Test 3: Multi-label in relationship pattern"
echo 'MATCH (u:User)-[:FOLLOWS]->(x:User|Post) RETURN x' | cargo run --bin clickgraph -- --sql-only 2>/dev/null && echo "✓ Multi-label in pattern works" || echo "✗ Multi-label in pattern failed"
echo ""

# Triple label
echo "Test 4: Triple label node"
echo 'MATCH (x:Person|User|Admin) RETURN x' | cargo run --bin clickgraph -- --sql-only 2>/dev/null && echo "✓ Triple label works" || echo "✗ Triple label failed"
echo ""

echo "=== Multi-Label Parsing Tests Complete ==="
echo ""
echo "Note: SQL generation may not fully support multi-labels yet (Part 1C-1D)"
echo "These tests verify the parser accepts the syntax."
