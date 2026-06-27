#!/usr/bin/env bash
# ============================================================================
# Experiment: canonical-graph-VIEW Cypher  vs  ClickGraph GENERATED SQL
#
# Question: would defining standard node_*/edge_* SQL VIEWs over the diverse
# physical schemas, then translating Cypher against those views, be efficient
# enough — vs ClickGraph's current compile-time inlining of the schema mapping?
#
# Method: for each schema pattern, run the SAME logical Cypher query two ways
# and compare the ClickHouse plan / rows read:
#   (A) GENERATED  = `cg sql` output (native-key joins, edge-collapse, anchored)
#   (B) VIEW       = naive Cypher-over-canonical-views SQL (synthetic id, etc.)
#
# Prereq: ClickHouse on :8123 (test_user/test_pass) with schema+data loaded:
#   docker exec <ch> clickhouse-client -u test_user --password test_pass \
#     --multiquery --queries-file benchmarks/schema_mapping_cost/setup.sql
#   CG=/path/to/cg  (cg built with --features databricks)
# ============================================================================
CH() { curl -s "http://localhost:8123/?user=test_user&password=test_pass&wait_end_of_query=1" "$@"; }
PLAN() { CH --data "EXPLAIN indexes=1 $1"; }
# total rows read across the whole query (incl. sub-scans), from the summary header
READ_ROWS() { CH -o /dev/null -D - --data "$1" | grep -i x-clickhouse-summary | grep -oE '"read_rows":"[0-9]+"' | grep -oE '[0-9]+'; }

CG=${CG:-/mnt/cargo-sd/cargo/target/debug/cg}
COMP=schemas/examples/composite_node_id_test.yaml
FK=schemas/examples/orders_customers_fk.yaml

echo "##################### CASE 1 — COMPOSITE-ID point join #####################"
echo "Cypher: MATCH (a1:Account)-[:TRANSFERRED]->(a2:Account) WHERE a1.bank_id=1 RETURN a2.holder_name,a2.balance"
echo "--- (A) GENERATED (cg sql) ---"; $CG sql --schema "$COMP" --dialect clickhouse \
  "MATCH (a1:Account)-[:TRANSFERRED]->(a2:Account) WHERE a1.bank_id=1 RETURN a2.holder_name,a2.balance"
GEN_C='SELECT a2.holder_name,a2.balance FROM db_composite_id.accounts a1
  JOIN db_composite_id.transfers t1 ON t1.from_bank_id=a1.bank_id AND t1.from_account_number=a1.account_number
  JOIN db_composite_id.accounts a2 ON a2.bank_id=t1.to_bank_id AND a2.account_number=t1.to_account_number
  WHERE a1.bank_id=1'
VIEW_C='SELECT a2.holder_name,a2.balance FROM db_composite_id.node_Account a1
  JOIN db_composite_id.edge_TRANSFERRED e ON e.start_id=a1.id
  JOIN db_composite_id.node_Account a2 ON a2.id=e.end_id WHERE a1.bank_id=1'
echo "transfers PK pruning — GENERATED:"; PLAN "$GEN_C" | grep -A8 "ReadFromMergeTree (db_composite_id.transfers)" | grep -E "Condition|Granules"
echo "transfers PK pruning — VIEW:";      PLAN "$VIEW_C" | grep -A8 "ReadFromMergeTree (db_composite_id.transfers)" | grep -E "Condition|Granules"

echo "##################### CASE 2 — FK-EDGE (edge IS the node table) #####################"
echo "Cypher: MATCH (o:Order)-[:PLACED_BY]->(c:Customer) WHERE c.customer_id=100 RETURN o.order_id"
echo "--- (A) GENERATED (cg sql) ---"; $CG sql --schema "$FK" --dialect clickhouse \
  "MATCH (o:Order)-[:PLACED_BY]->(c:Customer) WHERE c.name='Alice' RETURN o.order_id,o.total_amount"
GEN_F='SELECT o.order_id FROM test_integration.customers_fk c JOIN test_integration.orders_fk o ON c.customer_id=o.customer_id WHERE c.customer_id=100'
VIEW_F='SELECT o.order_id FROM test_integration.node_Order o JOIN test_integration.edge_PLACED_BY e ON e.start_id=o.id JOIN test_integration.node_Customer c ON c.id=e.end_id WHERE c.customer_id=100'
echo "base-table scans (ReadFromMergeTree) — GENERATED: $(PLAN "$GEN_F" | grep -c ReadFromMergeTree)   VIEW: $(PLAN "$VIEW_F" | grep -c ReadFromMergeTree)"

echo "##################### CASE 3 — VLP over COMPOSITE-ID #####################"
echo "Cypher: MATCH (a1:Account)-[:TRANSFERRED*1..3]->(a2:Account) WHERE a1.bank_id=1 RETURN a2.holder_name"
echo "Recursive CTEs are opaque to EXPLAIN (ReadFromRecursiveCTEStep) — compare read_rows."
GEN_V='WITH RECURSIVE vlp AS (
  SELECT s.bank_id sb, concat(toString(e.bank_id),0x7c,toString(e.account_number)) end_id, 1 hc, e.holder_name hn
  FROM db_composite_id.accounts s
  JOIN db_composite_id.transfers r ON s.bank_id=r.from_bank_id AND s.account_number=r.from_account_number
  JOIN db_composite_id.accounts e ON r.to_bank_id=e.bank_id AND r.to_account_number=e.account_number
  WHERE s.bank_id=1 AND s.account_number<50
  UNION ALL
  SELECT vp.sb, concat(toString(e.bank_id),0x7c,toString(e.account_number)), vp.hc+1, e.holder_name
  FROM vlp vp
  JOIN db_composite_id.transfers r ON vp.end_id=concat(toString(r.from_bank_id),0x7c,toString(r.from_account_number))
  JOIN db_composite_id.accounts e ON r.to_bank_id=e.bank_id AND r.to_account_number=e.account_number
  WHERE vp.hc<3
) SELECT count(),max(length(hn)) FROM vlp'
VIEW_V='WITH RECURSIVE vlp AS (
  SELECT a1.id sid, e.end_id end_id, 1 hc, a2.holder_name hn
  FROM db_composite_id.node_Account a1
  JOIN db_composite_id.edge_TRANSFERRED e ON e.start_id=a1.id
  JOIN db_composite_id.node_Account a2 ON a2.id=e.end_id
  WHERE a1.bank_id=1 AND a1.account_number<50
  UNION ALL
  SELECT vp.sid, e.end_id, vp.hc+1, a2.holder_name
  FROM vlp vp
  JOIN db_composite_id.edge_TRANSFERRED e ON e.start_id=vp.end_id
  JOIN db_composite_id.node_Account a2 ON a2.id=e.end_id
  WHERE vp.hc<3
) SELECT count(),max(length(hn)) FROM vlp'
echo "read_rows — GENERATED *1..3: $(READ_ROWS "$GEN_V")   VIEW *1..3: $(READ_ROWS "$VIEW_V")"
