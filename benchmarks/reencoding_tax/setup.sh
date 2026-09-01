#!/usr/bin/env bash
# ============================================================================
# Re-encoding-tax micro-benchmark — one-shot setup.
# Starts a PostgreSQL + Apache AGE container, loads the native (FK) side and the
# AGE (node/edge) side with identical data, then leaves it ready for
# bench_reencoding_tax.sh to time.  See README.md for the full story.
# ============================================================================
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
N="${N:-100000}"   # persons
D="${D:-20}"       # out-degree  ->  ~2,000,000 KNOWS edges at N=100000

echo ">>> (re)starting age-bench container (apache/age:latest = PostgreSQL 18 + AGE)"
docker rm -f age-bench >/dev/null 2>&1 || true
docker run -d --name age-bench --shm-size=2g \
  -e POSTGRES_PASSWORD=bench -e POSTGRES_USER=bench -e POSTGRES_DB=bench \
  -p 5455:5432 apache/age:latest >/dev/null
echo ">>> waiting for postgres..."
for _ in $(seq 1 30); do docker exec age-bench pg_isready -U bench >/dev/null 2>&1 && break; sleep 1; done
docker exec age-bench psql -U bench -d bench -t -c "SELECT version();" | head -1

echo ">>> loading NATIVE side (person + knows FK, N=$N D=$D)"
docker cp "$HERE/bench_native.sql" age-bench:/tmp/bench_native.sql
docker exec age-bench psql -U bench -d bench -q -v N="$N" -v D="$D" -f /tmp/bench_native.sql | tail -3

echo ">>> loading AGE side (social.\"Person\"/\"KNOWS\" from the native tables)"
docker exec age-bench psql -U bench -d bench -c "CREATE EXTENSION IF NOT EXISTS age;" >/dev/null
docker cp "$HERE/bench_age_load.sql" age-bench:/tmp/bench_age_load.sql
docker exec age-bench psql -U bench -d bench -q -f /tmp/bench_age_load.sql | tail -6

echo ">>> ready.  Now run:  bash $HERE/bench_reencoding_tax.sh"
