#!/bin/bash
# LDBC SNB Data Loading Benchmark
# Measures loading time for sf10 dataset with detailed metrics

set -e

SCALE_FACTOR="${1:-sf10}"
DATA_BASE="./data/${SCALE_FACTOR}/graphs/csv/interactive/composite-projected-fk"
CONTAINER="clickgraph-ldbc-clickhouse"
DATABASE="ldbc"
USER="default"
PASSWORD="default"
RESULTS_DIR="./benchmarks/ldbc_snb/results/loading"

mkdir -p "$RESULTS_DIR"
RESULTS_FILE="${RESULTS_DIR}/load_benchmark_${SCALE_FACTOR}_$(date +%Y%m%d_%H%M%S).json"
LOG_FILE="${RESULTS_DIR}/load_log_${SCALE_FACTOR}_$(date +%Y%m%d_%H%M%S).log"

echo "=========================================="
echo "LDBC SNB Data Loading Benchmark"
echo "=========================================="
echo "Scale Factor: $SCALE_FACTOR"
echo "Results: $RESULTS_FILE"
echo "=========================================="
echo "" | tee -a "$LOG_FILE"

# Check if data exists
if [ ! -d "$DATA_BASE" ]; then
    echo "❌ Data not found at: $DATA_BASE" | tee -a "$LOG_FILE"
    exit 1
fi

# Start timing
OVERALL_START=$(date +%s%N)

# Initialize results JSON
echo "{" > "$RESULTS_FILE"
echo "  \"scale_factor\": \"$SCALE_FACTOR\"," >> "$RESULTS_FILE"
echo "  \"timestamp\": \"$(date -Iseconds)\"," >> "$RESULTS_FILE"
echo "  \"tables\": [" >> "$RESULTS_FILE"

FIRST_TABLE=true

# Helper to load and benchmark a table
load_table_timed() {
    local table=$1
    local subdir=$2
    
    echo -n "Loading $table... " | tee -a "$LOG_FILE"
    
    # Find CSV files
    local csv_files=$(docker exec $CONTAINER find /data/${SCALE_FACTOR}/graphs/csv/interactive/composite-projected-fk -path "*/${subdir}/*.csv" 2>/dev/null | sort)
    
    if [ -z "$csv_files" ]; then
        echo "SKIP (no files)" | tee -a "$LOG_FILE"
        return
    fi
    
    # Start table timing
    local table_start=$(date +%s%N)
    
    # Truncate table
    docker exec $CONTAINER clickhouse-client \
        --user=$USER --password=$PASSWORD --database=$DATABASE \
        --query="TRUNCATE TABLE IF EXISTS $table" 2>/dev/null || true
    
    # Count files and total size
    local file_count=0
    local total_size=0
    
    # Load each CSV
    for csv_file in $csv_files; do
        ((file_count++))
        local file_size=$(docker exec $CONTAINER stat -c%s "$csv_file" 2>/dev/null || echo 0)
        ((total_size+=file_size))
        
        docker exec $CONTAINER clickhouse-client \
            --user=$USER --password=$PASSWORD --database=$DATABASE \
            --query="INSERT INTO $table FORMAT CSVWithNames SETTINGS format_csv_delimiter='|', input_format_csv_allow_variable_number_of_columns=1, input_format_skip_unknown_fields=1" \
            < <(docker exec $CONTAINER cat "$csv_file") 2>/dev/null || {
                echo "  ⚠️  Warning: Failed to load $csv_file" | tee -a "$LOG_FILE"
            }
    done
    
    # End table timing
    local table_end=$(date +%s%N)
    local duration_ms=$(( ($table_end - $table_start) / 1000000 ))
    local duration_sec=$(awk "BEGIN {printf \"%.2f\", $duration_ms/1000}")
    
    # Get row count and size
    local row_count=$(docker exec $CONTAINER clickhouse-client \
        --user=$USER --password=$PASSWORD --database=$DATABASE \
        --query="SELECT count() FROM $table" 2>/dev/null)
    
    local table_size=$(docker exec $CONTAINER clickhouse-client \
        --user=$USER --password=$PASSWORD --database=$DATABASE \
        --query="SELECT formatReadableSize(sum(bytes_on_disk)) FROM system.parts WHERE database='$DATABASE' AND table='$table' AND active" 2>/dev/null)
    
    # Calculate throughput
    local rows_per_sec=$(awk "BEGIN {printf \"%.0f\", $row_count/($duration_ms/1000)}")
    local mb_size=$(awk "BEGIN {printf \"%.2f\", $total_size/1048576}")
    local mb_per_sec=$(awk "BEGIN {printf \"%.2f\", $mb_size/($duration_ms/1000)}")
    
    echo "✅ ${row_count} rows, ${table_size}, ${duration_sec}s (${rows_per_sec} rows/s, ${mb_per_sec} MB/s)" | tee -a "$LOG_FILE"
    
    # Write to JSON
    if [ "$FIRST_TABLE" = false ]; then
        echo "    ," >> "$RESULTS_FILE"
    fi
    FIRST_TABLE=false
    
    cat >> "$RESULTS_FILE" << EOF
    {
      "table": "$table",
      "category": "$(dirname $subdir)",
      "rows": $row_count,
      "files": $file_count,
      "csv_size_mb": $mb_size,
      "storage_size": "$table_size",
      "duration_ms": $duration_ms,
      "duration_sec": $duration_sec,
      "rows_per_sec": $rows_per_sec,
      "mb_per_sec": $mb_per_sec
    }
EOF
}

echo "" | tee -a "$LOG_FILE"
echo "=== Loading Static Tables ===" | tee -a "$LOG_FILE"
load_table_timed "Organisation" "static/Organisation"
load_table_timed "Place" "static/Place"
load_table_timed "Tag" "static/Tag"
load_table_timed "TagClass" "static/TagClass"
load_table_timed "Organisation_isLocatedIn_Place" "static/Organisation_isLocatedIn_Place"
load_table_timed "Place_isPartOf_Place" "static/Place_isPartOf_Place"
load_table_timed "Tag_hasType_TagClass" "static/Tag_hasType_TagClass"
load_table_timed "TagClass_isSubclassOf_TagClass" "static/TagClass_isSubclassOf_TagClass"

echo "" | tee -a "$LOG_FILE"
echo "=== Loading Dynamic Node Tables ===" | tee -a "$LOG_FILE"
load_table_timed "Person" "dynamic/Person"
load_table_timed "Forum" "dynamic/Forum"
load_table_timed "Post" "dynamic/Post"
load_table_timed "Comment" "dynamic/Comment"

echo "" | tee -a "$LOG_FILE"
echo "=== Loading Person Relationships ===" | tee -a "$LOG_FILE"
load_table_timed "Person_knows_Person" "dynamic/Person_knows_Person"
load_table_timed "Person_hasInterest_Tag" "dynamic/Person_hasInterest_Tag"
load_table_timed "Person_isLocatedIn_City" "dynamic/Person_isLocatedIn_City"
load_table_timed "Person_likes_Comment" "dynamic/Person_likes_Comment"
load_table_timed "Person_likes_Post" "dynamic/Person_likes_Post"
load_table_timed "Person_studyAt_University" "dynamic/Person_studyAt_University"
load_table_timed "Person_workAt_Company" "dynamic/Person_workAt_Company"

echo "" | tee -a "$LOG_FILE"
echo "=== Loading Forum Relationships ===" | tee -a "$LOG_FILE"
load_table_timed "Forum_containerOf_Post" "dynamic/Forum_containerOf_Post"
load_table_timed "Forum_hasMember_Person" "dynamic/Forum_hasMember_Person"
load_table_timed "Forum_hasModerator_Person" "dynamic/Forum_hasModerator_Person"
load_table_timed "Forum_hasTag_Tag" "dynamic/Forum_hasTag_Tag"

echo "" | tee -a "$LOG_FILE"
echo "=== Loading Post Relationships ===" | tee -a "$LOG_FILE"
load_table_timed "Post_hasCreator_Person" "dynamic/Post_hasCreator_Person"
load_table_timed "Post_hasTag_Tag" "dynamic/Post_hasTag_Tag"
load_table_timed "Post_isLocatedIn_Country" "dynamic/Post_isLocatedIn_Country"

echo "" | tee -a "$LOG_FILE"
echo "=== Loading Comment Relationships ===" | tee -a "$LOG_FILE"
load_table_timed "Comment_hasCreator_Person" "dynamic/Comment_hasCreator_Person"
load_table_timed "Comment_hasTag_Tag" "dynamic/Comment_hasTag_Tag"
load_table_timed "Comment_isLocatedIn_Country" "dynamic/Comment_isLocatedIn_Country"
load_table_timed "Comment_replyOf_Comment" "dynamic/Comment_replyOf_Comment"
load_table_timed "Comment_replyOf_Post" "dynamic/Comment_replyOf_Post"

# End overall timing
OVERALL_END=$(date +%s%N)
OVERALL_DURATION_MS=$(( ($OVERALL_END - $OVERALL_START) / 1000000 ))
OVERALL_DURATION_SEC=$(awk "BEGIN {printf \"%.2f\", $OVERALL_DURATION_MS/1000}")

# Close tables array
echo "" >> "$RESULTS_FILE"
echo "  ]," >> "$RESULTS_FILE"

# Get summary stats
TOTAL_ROWS=$(docker exec $CONTAINER clickhouse-client \
    --user=$USER --password=$PASSWORD \
    --query="SELECT sum(total_rows) FROM system.tables WHERE database = '$DATABASE'" 2>/dev/null)

TOTAL_SIZE=$(docker exec $CONTAINER clickhouse-client \
    --user=$USER --password=$PASSWORD \
    --query="SELECT formatReadableSize(sum(bytes_on_disk)) FROM system.parts WHERE database = '$DATABASE' AND active" 2>/dev/null)

TABLES_LOADED=$(docker exec $CONTAINER clickhouse-client \
    --user=$USER --password=$PASSWORD \
    --query="SELECT count() FROM system.tables WHERE database = '$DATABASE' AND total_rows > 0" 2>/dev/null)

# Write summary
cat >> "$RESULTS_FILE" << EOF
  "summary": {
    "total_duration_ms": $OVERALL_DURATION_MS,
    "total_duration_sec": $OVERALL_DURATION_SEC,
    "tables_loaded": $TABLES_LOADED,
    "total_rows": $TOTAL_ROWS,
    "total_size": "$TOTAL_SIZE",
    "avg_rows_per_sec": $(awk "BEGIN {printf \"%.0f\", $TOTAL_ROWS/($OVERALL_DURATION_MS/1000)}"),
    "system_info": {
      "clickhouse_version": "$(docker exec $CONTAINER clickhouse-client --version | head -1)",
      "cpu_cores": $(nproc),
      "memory_gb": $(free -g | awk '/^Mem:/{print $2}')
    }
  }
}
EOF

echo "" | tee -a "$LOG_FILE"
echo "==========================================" | tee -a "$LOG_FILE"
echo "✅ Load Complete!" | tee -a "$LOG_FILE"
echo "==========================================" | tee -a "$LOG_FILE"
echo "Total Duration: ${OVERALL_DURATION_SEC}s" | tee -a "$LOG_FILE"
echo "Tables Loaded: $TABLES_LOADED" | tee -a "$LOG_FILE"
echo "Total Rows: $TOTAL_ROWS" | tee -a "$LOG_FILE"
echo "Total Size: $TOTAL_SIZE" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "Results saved to: $RESULTS_FILE" | tee -a "$LOG_FILE"
echo "Log saved to: $LOG_FILE" | tee -a "$LOG_FILE"

# Show top tables
echo "" | tee -a "$LOG_FILE"
echo "Top 10 tables by row count:" | tee -a "$LOG_FILE"
docker exec $CONTAINER clickhouse-client \
    --user=$USER --password=$PASSWORD \
    --query="SELECT 
        table, 
        formatReadableSize(total_bytes) as size, 
        total_rows 
    FROM system.tables 
    WHERE database = '$DATABASE' AND total_rows > 0 
    ORDER BY total_rows DESC 
    LIMIT 10" | tee -a "$LOG_FILE"
