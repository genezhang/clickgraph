#!/bin/bash
# Setup all test databases for schema variation testing
#
# Creates all databases needed for testing GraphRAG query patterns across
# different schema variations:
#   - db_standard      — Standard schema (separate node + edge tables)
#   - db_fk_edge       — FK-edge pattern (FK column as relationship)
#   - db_denormalized  — Denormalized (node props in edge table)
#   - db_polymorphic   — Polymorphic edge (type_column discriminator)
#   - db_composite_id  — Composite node ID (multi-column identity)
#   - zeek             — Coupled edges (multiple relationships from one table)
#
# Usage:
#   CLICKHOUSE_USER=test_user CLICKHOUSE_PASSWORD=test_pass bash scripts/setup/setup_all_test_databases.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "========================================="
echo "  Setting up all test databases"
echo "========================================="
echo ""

# Setup standard schema databases
bash "${SCRIPT_DIR}/setup_standard_data.sh"
echo ""

bash "${SCRIPT_DIR}/setup_fk_edge_data.sh"
echo ""

bash "${SCRIPT_DIR}/setup_denormalized_data.sh"
echo ""

bash "${SCRIPT_DIR}/setup_polymorphic_data.sh"
echo ""

bash "${SCRIPT_DIR}/setup_composite_id_data.sh"
echo ""

# Setup zeek database for coupled edges testing
bash "${SCRIPT_DIR}/setup_zeek_data.sh"
echo ""

echo "========================================="
echo "  All test databases ready!"
echo "========================================="
echo ""
echo "Databases created:"
echo "  db_standard     → Standard schema (separate node + edge tables)"
echo "  db_fk_edge      → FK-edge pattern (FK column as relationship)"
echo "  db_denormalized → Denormalized (node props in edge table)"
echo "  db_polymorphic  → Polymorphic edge (type_column discriminator)"
echo "  db_composite_id → Composite node ID (multi-column identity)"
echo "  zeek            → Coupled edges (multiple relationships from one table)"
echo ""
echo "Start server with:"
echo "  GRAPH_CONFIG_PATH=schemas/test/unified_test_multi_schema.yaml cargo run --bin clickgraph"
