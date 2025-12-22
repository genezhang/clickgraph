#!/bin/bash
# Load small OnTime flights test data
set -e

cd "$(dirname "$0")/../.."

echo "Loading OnTime test data..."
docker exec -i clickhouse clickhouse-client --multiquery < tests/fixtures/data/ontime_test_data.sql

echo "Verifying data..."
docker exec clickhouse clickhouse-client -q "SELECT 'Flights loaded:' AS status, count(*) AS count FROM default.flights FORMAT PrettyCompact"
docker exec clickhouse clickhouse-client -q "SELECT 'Airports (origins):' AS status, count(DISTINCT OriginAirportID) AS count FROM default.flights FORMAT PrettyCompact"
docker exec clickhouse clickhouse-client -q "SELECT 'Airlines:' AS status, count(DISTINCT IATA_CODE_Reporting_Airline) AS count FROM default.flights FORMAT PrettyCompact"

echo "✅ OnTime test data loaded successfully!"
