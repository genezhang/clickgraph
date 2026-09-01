#!/usr/bin/env bash
# ============================================================================
# Re-encoding-tax micro-benchmark — timing harness (Table 6 of the tech report)
#
# Question (Claim B, "representation"): on ONE engine, holding data and hardware
# fixed and changing only the representation, how much slower is the same graph
# re-encoded into generic node/edge tables (Apache AGE) than native FK-join SQL?
#
# Method: identical data loaded two ways into the same PostgreSQL+AGE container —
#   (A) NATIVE  = person + knows(src,dst) with indexed FK, answered by FK-join SQL
#   (B) AGE     = social."Person"/"KNOWS" node/edge tables, answered by Cypher
# The three queries return byte-identical counts on both sides (verified at load).
# Wall-clock server time via psql \timing, median of 5 runs.
#
# Prereq: run setup.sh first (starts age-bench, loads both sides). Then:
#   bash bench_reencoding_tax.sh
# ============================================================================
set -u
AGE_PRE="LOAD 'age'; SET search_path=ag_catalog,\"\$user\",public;"

timeq() {  # $1=label  $2=full-sql
  local label="$1"; local sql="$2"
  local times=()
  for i in 1 2 3 4 5; do
    # \timing prints "Time: N ms" to stdout; capture the query's line (last)
    t=$(docker exec age-bench psql -U bench -d bench -q -c "\timing on" -c "$sql" 2>/dev/null | grep -oP 'Time: \K[0-9.]+' | tail -1)
    times+=("$t")
  done
  printf '%s\t' "$label"
  printf '%s\n' "${times[@]}" | sort -n | awk 'NR==3{printf "%.1f ms\n",$1}'
}

echo "=== NATIVE relational (FK-join SQL) — same engine, same data ==="
timeq "Q1 native 2-hop (seed)"   "SELECT count(*) FROM knows k1 JOIN knows k2 ON k1.dst=k2.src WHERE k1.src=42"
timeq "Q2 native 3-hop (seed)"   "SELECT count(*) FROM knows k1 JOIN knows k2 ON k1.dst=k2.src JOIN knows k3 ON k2.dst=k3.src WHERE k1.src=42"
timeq "Q3 native 2-hop (global)" "SELECT count(*) FROM knows k1 JOIN knows k2 ON k1.dst=k2.src"

echo ""
echo "=== AGE re-encoded (Cypher over node/edge) — same engine, same data ==="
timeq "Q1 AGE 2-hop (seed)"   "${AGE_PRE} SELECT * FROM cypher('social',\$\$ MATCH (a:Person {id:42})-[:KNOWS]->()-[:KNOWS]->(c) RETURN count(c) \$\$) AS (n agtype)"
timeq "Q2 AGE 3-hop (seed)"   "${AGE_PRE} SELECT * FROM cypher('social',\$\$ MATCH (a:Person {id:42})-[:KNOWS]->()-[:KNOWS]->()-[:KNOWS]->(d) RETURN count(d) \$\$) AS (n agtype)"
timeq "Q3 AGE 2-hop (global)" "${AGE_PRE} SELECT * FROM cypher('social',\$\$ MATCH (a:Person)-[:KNOWS]->()-[:KNOWS]->(c) RETURN count(c) \$\$) AS (n agtype)"
