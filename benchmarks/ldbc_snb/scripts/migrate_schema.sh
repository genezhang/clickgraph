#!/bin/bash
# Migrate LDBC tables to corrected schema
# This drops and recreates tables with correct column names matching CSV headers

set -e

DATABASE="ldbc"
USER="default"
PASSWORD="default"

CH_CLIENT="clickhouse-client --user=$USER --password=$PASSWORD --database=$DATABASE"

echo "=========================================="
echo "LDBC Schema Migration to Corrected DDL"
echo "=========================================="
echo ""
echo "⚠️  WARNING: This will DROP and RECREATE all tables"
echo "⚠️  All existing data will be lost"
echo ""
read -p "Continue? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 1
fi

echo ""
echo "Dropping existing tables..."
$CH_CLIENT --query="DROP TABLE IF EXISTS Person_likes_Comment"
$CH_CLIENT --query="DROP TABLE IF EXISTS Person_likes_Post"
$CH_CLIENT --query="DROP TABLE IF EXISTS Comment_replyOf_Comment"
$CH_CLIENT --query="DROP TABLE IF EXISTS Comment_replyOf_Post"
$CH_CLIENT --query="DROP TABLE IF EXISTS Comment_hasTag_Tag"
$CH_CLIENT --query="DROP TABLE IF EXISTS Comment_isLocatedIn_Place"
$CH_CLIENT --query="DROP TABLE IF EXISTS Comment_hasCreator_Person"
$CH_CLIENT --query="DROP TABLE IF EXISTS Comment"
$CH_CLIENT --query="DROP TABLE IF EXISTS Forum_containerOf_Post"
$CH_CLIENT --query="DROP TABLE IF EXISTS Post_hasTag_Tag"
$CH_CLIENT --query="DROP TABLE IF EXISTS Post_isLocatedIn_Place"
$CH_CLIENT --query="DROP TABLE IF EXISTS Post_hasCreator_Person"
$CH_CLIENT --query="DROP TABLE IF EXISTS Post"
$CH_CLIENT --query="DROP TABLE IF EXISTS Forum_hasTag_Tag"
$CH_CLIENT --query="DROP TABLE IF EXISTS Forum_hasMember_Person"
$CH_CLIENT --query="DROP TABLE IF EXISTS Forum_hasModerator_Person"
$CH_CLIENT --query="DROP TABLE IF EXISTS Forum"
$CH_CLIENT --query="DROP TABLE IF EXISTS Person_knows_Person"
$CH_CLIENT --query="DROP TABLE IF EXISTS Person_studyAt_Organisation"
$CH_CLIENT --query="DROP TABLE IF EXISTS Person_workAt_Organisation"
$CH_CLIENT --query="DROP TABLE IF EXISTS Person_hasInterest_Tag"
$CH_CLIENT --query="DROP TABLE IF EXISTS Person_isLocatedIn_Place"
$CH_CLIENT --query="DROP TABLE IF EXISTS Person"
$CH_CLIENT --query="DROP TABLE IF EXISTS TagClass_isSubclassOf_TagClass"
$CH_CLIENT --query="DROP TABLE IF EXISTS Tag_hasType_TagClass"
$CH_CLIENT --query="DROP TABLE IF EXISTS TagClass"
$CH_CLIENT --query="DROP TABLE IF EXISTS Tag"
$CH_CLIENT --query="DROP TABLE IF EXISTS Organisation_isLocatedIn_Place"
$CH_CLIENT --query="DROP TABLE IF EXISTS Organisation"
$CH_CLIENT --query="DROP TABLE IF EXISTS Place_isPartOf_Place"
$CH_CLIENT --query="DROP TABLE IF EXISTS Place"
$CH_CLIENT --query="DROP VIEW IF EXISTS Person_likes_Message"
$CH_CLIENT --query="DROP VIEW IF EXISTS Message_hasCreator_Person"
$CH_CLIENT --query="DROP VIEW IF EXISTS Message"

echo "✓ All tables dropped"
echo ""
echo "Creating tables with corrected schema..."

# Execute the corrected DDL
$CH_CLIENT < /home/gz/clickgraph/benchmarks/ldbc_snb/schemas/clickhouse_ddl.sql

echo "✓ Tables created with corrected schema"
echo ""
echo "=========================================="
echo "Migration complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Run load_data_docker_tsv.sh to reload all data"
echo "2. Verify data with: clickhouse-client --query='SELECT count() FROM Person'"
