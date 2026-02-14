#!/bin/bash
# Setup all db_xxx test databases for browser testing
#
# Creates self-contained databases for each schema variation:
#   - db_standard      — Standard schema (User, Post, FOLLOWS, AUTHORED, LIKED, FRIENDS_WITH)
#   - db_fk_edge       — FK-edge pattern (Order→Customer via FK column)
#   - db_denormalized  — Denormalized (Airport embedded in flights table)
#   - db_polymorphic   — Polymorphic edge (interactions table with type_column)
#   - db_composite_id  — Composite node ID (Account=[bank_id, account_number])
#
# Each database is isolated and can be used independently.
# Corresponding schemas are in schemas/dev/

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "========================================="
echo "  Setting up all db_xxx test databases"
echo "========================================="
echo ""

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

echo "========================================="
echo "  All db_xxx databases ready!"
echo "========================================="
echo ""
echo "Databases created:"
echo "  db_standard     → schemas/dev/social_standard.yaml"
echo "  db_fk_edge      → schemas/dev/orders_customers_fk.yaml"
echo "  db_denormalized → schemas/dev/flights_denormalized.yaml"
echo "  db_polymorphic  → schemas/dev/social_polymorphic.yaml"
echo "  db_composite_id → schemas/examples/composite_node_id_test.yaml"
