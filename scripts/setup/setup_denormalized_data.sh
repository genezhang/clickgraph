#!/bin/bash
# Setup test data for denormalized schema (node properties embedded in edge table)
# Database: db_denormalized
# Schema: schemas/dev/flights_denormalized.yaml
#
# Tables:
#   - flights_denorm (single table: edge=FLIGHT, node=Airport is virtual)
#
# Denormalized pattern: Airport node properties (city, state) are embedded
# directly in the flights table as origin_city/dest_city, origin_state/dest_state.
# No separate Airport table exists.

set -e

CH_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CH_USER="${CLICKHOUSE_USER:-test_user}"
CH_PASS="${CLICKHOUSE_PASSWORD:-test_pass}"

run_sql() {
    echo "$1" | curl -s "${CH_URL}/?user=${CH_USER}&password=${CH_PASS}" --data-binary @-
}

echo "=== Setting up db_denormalized ==="

run_sql "CREATE DATABASE IF NOT EXISTS db_denormalized"

# Flights table (denormalized: contains Airport node properties)
run_sql "CREATE TABLE IF NOT EXISTS db_denormalized.flights_denorm (
    flight_id UInt32,
    flight_number String,
    carrier String,
    origin_code String,
    origin_city String,
    origin_state String,
    dest_code String,
    dest_city String,
    dest_state String,
    departure_time String,
    arrival_time String,
    distance UInt32
) ENGINE = Memory"

# Clear existing data
run_sql "TRUNCATE TABLE IF EXISTS db_denormalized.flights_denorm"

echo "Tables created. Inserting data..."

run_sql "INSERT INTO db_denormalized.flights_denorm VALUES
(1, 'AA100', 'American', 'LAX', 'Los Angeles', 'CA', 'JFK', 'New York', 'NY', '08:00', '16:30', 2475),
(2, 'UA200', 'United', 'SFO', 'San Francisco', 'CA', 'ORD', 'Chicago', 'IL', '09:00', '15:00', 1846),
(3, 'DL300', 'Delta', 'ATL', 'Atlanta', 'GA', 'LAX', 'Los Angeles', 'CA', '10:00', '12:30', 1946),
(4, 'SW400', 'Southwest', 'DEN', 'Denver', 'CO', 'PHX', 'Phoenix', 'AZ', '11:00', '12:30', 602),
(5, 'AA101', 'American', 'JFK', 'New York', 'NY', 'LAX', 'Los Angeles', 'CA', '14:00', '17:30', 2475),
(6, 'UA201', 'United', 'ORD', 'Chicago', 'IL', 'DEN', 'Denver', 'CO', '13:00', '14:30', 888),
(7, 'DL301', 'Delta', 'LAX', 'Los Angeles', 'CA', 'ATL', 'Atlanta', 'GA', '15:00', '22:00', 1946),
(8, 'AA102', 'American', 'LAX', 'Los Angeles', 'CA', 'ORD', 'Chicago', 'IL', '07:00', '13:00', 1745)"

echo ""
echo "=== Data loaded ==="
echo "Flights: $(run_sql 'SELECT count() FROM db_denormalized.flights_denorm')"
echo ""
echo "Start server with:"
echo "  GRAPH_CONFIG_PATH=schemas/dev/flights_denormalized.yaml cargo run --bin clickgraph"
