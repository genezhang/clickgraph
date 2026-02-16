#!/bin/bash
set -e

# Wait for ClickHouse to be fully ready
sleep 2

clickhouse-client --multiquery < /docker-entrypoint-initdb.d/init-db.sql
