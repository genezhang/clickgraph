# LDBC SNB Data Loading Guide

This guide explains how to load and persist LDBC SNB benchmark data in ClickHouse with proper volume management.

## Quick Start

```bash
# 1. Start ClickHouse with persistent volume
cd benchmarks/ldbc_snb
docker-compose -f docker-compose.ldbc.yaml up -d

# 2. Download sf10 data (if not already present)
bash ./scripts/download_data.sh sf10

# 3. Load data into ClickHouse
bash ./scripts/load_sf10_data.sh

# 4. Verify data loaded
docker exec clickgraph-ldbc-clickhouse clickhouse-client \
  --user=default --password=default \
  --query "SELECT table, formatReadableSize(total_bytes) as size, total_rows FROM system.tables WHERE database = 'ldbc' AND total_rows > 0 ORDER BY total_rows DESC LIMIT 10"
```

## Volume Management

### Current Setup

The `docker-compose.ldbc.yaml` uses a **named Docker volume** for persistence:

```yaml
volumes:
  - ldbc_clickhouse_data:/var/lib/clickhouse  # Persistent volume
  - ./data:/data:ro                           # Read-only data mount
```

**Volume name**: `ldbc_snb_ldbc_clickhouse_data`  
**Location**: `/var/lib/docker/volumes/ldbc_snb_ldbc_clickhouse_data/_data`

### Volume Operations

```bash
# List LDBC volumes
docker volume ls | grep ldbc

# Inspect volume
docker volume inspect ldbc_snb_ldbc_clickhouse_data

# Check volume size
sudo du -sh /var/lib/docker/volumes/ldbc_snb_ldbc_clickhouse_data/_data

# Backup volume (recommended before major operations)
docker run --rm -v ldbc_snb_ldbc_clickhouse_data:/data -v $(pwd)/backup:/backup \
  alpine tar czf /backup/ldbc_clickhouse_backup_$(date +%Y%m%d).tar.gz -C /data .

# Restore from backup
docker run --rm -v ldbc_snb_ldbc_clickhouse_data:/data -v $(pwd)/backup:/backup \
  alpine tar xzf /backup/ldbc_clickhouse_backup_YYYYMMDD.tar.gz -C /data
```

### Clean Start (if needed)

```bash
# Stop container
docker-compose -f docker-compose.ldbc.yaml down

# Remove volume (WARNING: deletes all data)
docker volume rm ldbc_snb_ldbc_clickhouse_data

# Start fresh
docker-compose -f docker-compose.ldbc.yaml up -d

# Reload data
bash ./scripts/load_sf10_data.sh
```

## Data Loading Process

### Scale Factors Available

- **sf0.003**: ~3K persons, ~33K edges - For quick tests
- **sf0.1**: ~10K persons, ~100K edges - Small benchmark  
- **sf1**: ~10K persons, ~1M edges - Medium benchmark
- **sf10**: ~100K persons, ~10M edges - **Recommended for benchmarks**
- **sf30**: ~300K persons, ~30M edges - Large benchmark
- **sf100**: ~1M persons, ~100M edges - Production scale

### Load sf10 Data

Create `scripts/load_sf10_data.sh`:

```bash
#!/bin/bash
# Load LDBC SNB sf10 data into ClickHouse with proper error handling

set -e

SCALE_FACTOR="sf10"
DATA_DIR="./data/${SCALE_FACTOR}/graphs/csv/interactive/composite-projected-fk"
CONTAINER="clickgraph-ldbc-clickhouse"
DATABASE="ldbc"
USER="default"
PASSWORD="default"

echo "=========================================="
echo "Loading LDBC SNB ${SCALE_FACTOR} Data"
echo "=========================================="

# Check if data exists
if [ ! -d "$DATA_DIR" ]; then
    echo "❌ Data not found at: $DATA_DIR"
    echo "Run: bash ./scripts/download_data.sh $SCALE_FACTOR"
    exit 1
fi

# Helper to load a table
load_table() {
    local table=$1
    local csv_pattern=$2
    
    echo -n "Loading $table... "
    
    # Find CSV files
    csv_files=$(docker exec $CONTAINER find /data/${SCALE_FACTOR}/graphs/csv/interactive/composite-projected-fk -path "*/${csv_pattern}/*.csv" 2>/dev/null || true)
    
    if [ -z "$csv_files" ]; then
        echo "SKIP (no files)"
        return
    fi
    
    # Truncate table
    docker exec $CONTAINER clickhouse-client \
        --user=$USER --password=$PASSWORD --database=$DATABASE \
        --query="TRUNCATE TABLE IF EXISTS $table" 2>/dev/null || true
    
    # Load each CSV
    for csv_file in $csv_files; do
        docker exec $CONTAINER clickhouse-client \
            --user=$USER --password=$PASSWORD --database=$DATABASE \
            --query="INSERT INTO $table FORMAT CSVWithNames SETTINGS format_csv_delimiter='|', input_format_csv_allow_variable_number_of_columns=1" \
            < "$csv_file" 2>/dev/null || echo "  Warning: Failed to load $csv_file"
    done
    
    # Get count
    count=$(docker exec $CONTAINER clickhouse-client \
        --user=$USER --password=$PASSWORD --database=$DATABASE \
        --query="SELECT count() FROM $table" 2>/dev/null)
    
    echo "✅ $count rows"
}

# Load static data
echo ""
echo "=== Loading Static Tables ==="
load_table "Organisation" "static/Organisation"
load_table "Place" "static/Place"
load_table "Tag" "static/Tag"
load_table "TagClass" "static/TagClass"

# Load static relationships
load_table "Organisation_isLocatedIn_Place" "static/Organisation_isLocatedIn_Place"
load_table "Place_isPartOf_Place" "static/Place_isPartOf_Place"
load_table "Tag_hasType_TagClass" "static/Tag_hasType_TagClass"
load_table "TagClass_isSubclassOf_TagClass" "static/TagClass_isSubclassOf_TagClass"

# Load dynamic data (nodes)
echo ""
echo "=== Loading Dynamic Node Tables ==="
load_table "Person" "dynamic/Person"
load_table "Forum" "dynamic/Forum"
load_table "Post" "dynamic/Post"
load_table "Comment" "dynamic/Comment"

# Load dynamic relationships
echo ""
echo "=== Loading Dynamic Relationship Tables ==="
load_table "Person_knows_Person" "dynamic/Person_knows_Person"
load_table "Person_hasInterest_Tag" "dynamic/Person_hasInterest_Tag"
load_table "Person_isLocatedIn_City" "dynamic/Person_isLocatedIn_City"
load_table "Person_likes_Comment" "dynamic/Person_likes_Comment"
load_table "Person_likes_Post" "dynamic/Person_likes_Post"
load_table "Person_studyAt_University" "dynamic/Person_studyAt_University"
load_table "Person_workAt_Company" "dynamic/Person_workAt_Company"

load_table "Forum_containerOf_Post" "dynamic/Forum_containerOf_Post"
load_table "Forum_hasMember_Person" "dynamic/Forum_hasMember_Person"
load_table "Forum_hasModerator_Person" "dynamic/Forum_hasModerator_Person"
load_table "Forum_hasTag_Tag" "dynamic/Forum_hasTag_Tag"

load_table "Post_hasCreator_Person" "dynamic/Post_hasCreator_Person"
load_table "Post_hasTag_Tag" "dynamic/Post_hasTag_Tag"
load_table "Post_isLocatedIn_Country" "dynamic/Post_isLocatedIn_Country"

load_table "Comment_hasCreator_Person" "dynamic/Comment_hasCreator_Person"
load_table "Comment_hasTag_Tag" "dynamic/Comment_hasTag_Tag"
load_table "Comment_isLocatedIn_Country" "dynamic/Comment_isLocatedIn_Country"
load_table "Comment_replyOf_Comment" "dynamic/Comment_replyOf_Comment"
load_table "Comment_replyOf_Post" "dynamic/Comment_replyOf_Post"

echo ""
echo "=========================================="
echo "✅ Load Complete!"
echo "=========================================="

# Show summary
docker exec $CONTAINER clickhouse-client \
    --user=$USER --password=$PASSWORD \
    --query="SELECT 
        'Total Tables' as metric, 
        count() as value 
    FROM system.tables 
    WHERE database = '$DATABASE' AND total_rows > 0
    UNION ALL
    SELECT 
        'Total Rows' as metric, 
        sum(total_rows) as value 
    FROM system.tables 
    WHERE database = '$DATABASE'
    UNION ALL
    SELECT 
        'Total Size' as metric, 
        formatReadableSize(sum(total_bytes)) as value 
    FROM system.tables 
    WHERE database = '$DATABASE'"

echo ""
echo "Top tables by row count:"
docker exec $CONTAINER clickhouse-client \
    --user=$USER --password=$PASSWORD \
    --query="SELECT 
        table, 
        formatReadableSize(total_bytes) as size, 
        total_rows 
    FROM system.tables 
    WHERE database = '$DATABASE' AND total_rows > 0 
    ORDER BY total_rows DESC 
    LIMIT 10"
```

Make it executable:
```bash
chmod +x ./scripts/load_sf10_data.sh
```

## Data Verification

### Check What's Loaded

```bash
# Count rows in all tables
docker exec clickgraph-ldbc-clickhouse clickhouse-client \
  --user=default --password=default \
  --query="SELECT table, total_rows FROM system.tables WHERE database = 'ldbc' AND total_rows > 0 ORDER BY total_rows DESC"

# Check node tables specifically
docker exec clickgraph-ldbc-clickhouse clickhouse-client \
  --user=default --password=default \
  --query="SELECT 
    (SELECT count() FROM ldbc.Person) as persons,
    (SELECT count() FROM ldbc.Forum) as forums,
    (SELECT count() FROM ldbc.Post) as posts,
    (SELECT count() FROM ldbc.Comment) as comments"

# Check total database size
docker exec clickgraph-ldbc-clickhouse clickhouse-client \
  --user=default --password=default \
  --query="SELECT 
    formatReadableSize(sum(bytes_on_disk)) as total_size,
    sum(rows) as total_rows
  FROM system.parts 
  WHERE database = 'ldbc' AND active"
```

### Test Queries

```bash
# Start ClickGraph server
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD="default"
export CLICKHOUSE_DATABASE="ldbc"
export GRAPH_CONFIG_PATH="./benchmarks/ldbc_snb/schemas/ldbc_snb.yaml"

cargo run --release > /tmp/clickgraph_ldbc.log 2>&1 &

# Wait for server
sleep 5

# Run quick tests
bash ./scripts/quick_test.sh

# Run comprehensive tests
bash ./scripts/test_working_queries.sh
```

## Troubleshooting

### Data Not Persisting

**Problem**: Data disappears after container restart

**Solution**: Check volume is mounted correctly
```bash
docker inspect clickgraph-ldbc-clickhouse | jq '.[0].Mounts[] | select(.Destination == "/var/lib/clickhouse")'
```

Should show:
```json
{
  "Type": "volume",
  "Name": "ldbc_snb_ldbc_clickhouse_data",
  "Source": "/var/lib/docker/volumes/ldbc_snb_ldbc_clickhouse_data/_data",
  "Destination": "/var/lib/clickhouse",
  "Driver": "local",
  "Mode": "z",
  "RW": true,
  "Propagation": ""
}
```

### Node Tables Empty

**Problem**: Relationship tables have data but Person/Post/Comment are empty

**Solution**: Reload node tables explicitly
```bash
# Truncate and reload Person table
docker exec clickgraph-ldbc-clickhouse clickhouse-client \
  --user=default --password=default \
  --query="TRUNCATE TABLE ldbc.Person"

# Find and load CSV files
find ./data/sf10/graphs/csv/interactive/composite-projected-fk/dynamic/Person -name "*.csv" -exec \
  docker exec -i clickgraph-ldbc-clickhouse clickhouse-client \
  --user=default --password=default --database=ldbc \
  --query="INSERT INTO Person FORMAT CSVWithNames SETTINGS format_csv_delimiter='|'" < {} \;
```

### CSV Import Errors

**Problem**: `Code: 27. DB::Exception: Cannot parse CSV`

**Solution**: Check delimiter and format
```bash
# Verify CSV format
head -5 ./data/sf10/graphs/csv/interactive/composite-projected-fk/dynamic/Person/part-00000-*.csv

# Try different settings
docker exec -i clickgraph-ldbc-clickhouse clickhouse-client \
  --user=default --password=default --database=ldbc \
  --query="INSERT INTO Person FORMAT CSVWithNames SETTINGS 
    format_csv_delimiter='|',
    input_format_csv_allow_variable_number_of_columns=1,
    input_format_skip_unknown_fields=1" \
  < ./data/sf10/.../Person/part-00000-*.csv
```

## Expected Data Sizes

| Scale Factor | Persons | Edges | Disk Size (approx) |
|--------------|---------|-------|--------------------|
| sf0.003 | 327 | 3.3K | ~5 MB |
| sf0.1 | 10K | 100K | ~150 MB |
| sf1 | 11K | 1.1M | ~1.5 GB |
| sf10 | 73K | 7.3M | ~15 GB |
| sf30 | 220K | 22M | ~45 GB |
| sf100 | 730K | 73M | ~150 GB |

## Performance Tips

1. **Use MergeTree for production**: Edit `schemas/clickhouse_ddl.sql` to use `ENGINE = MergeTree ORDER BY id` instead of `Memory`

2. **Optimize before benchmarking**:
```sql
OPTIMIZE TABLE ldbc.Person FINAL;
OPTIMIZE TABLE ldbc.Post FINAL;
-- etc for all tables
```

3. **Monitor memory usage**:
```sql
SELECT 
    formatReadableSize(value) as memory 
FROM system.metrics 
WHERE metric = 'MemoryTracking';
```

4. **Check query performance**:
```sql
SYSTEM FLUSH LOGS;
SELECT 
    query_duration_ms,
    query,
    read_rows,
    formatReadableSize(read_bytes) as read_size
FROM system.query_log 
WHERE type = 'QueryFinish' 
ORDER BY query_start_time DESC 
LIMIT 10;
```
