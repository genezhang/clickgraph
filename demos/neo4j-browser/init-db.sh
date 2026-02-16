#!/bin/bash
# Initialize demo database with tables and data
clickhouse-client --query "$(cat /docker-entrypoint-initdb.d/init-db.sql)"
