#!/bin/bash
# Load LDBC SNB data via Docker - TSV conversion approach
# This handles the embedded comma issue in LDBC data
# Usage: ./load_data_docker_tsv.sh sf10

set -e

SCALE_FACTOR=${1:-sf0.003}
DATA_BASE="/data/${SCALE_FACTOR}/graphs/csv/interactive/composite-projected-fk"
DATABASE="ldbc"
USER="default"
PASSWORD="default"

CH_CLIENT="clickhouse-client --user=$USER --password=$PASSWORD --database=$DATABASE"

echo "=========================================="
echo "LDBC SNB Data Loader (TSV conversion)"
echo "=========================================="
echo "Scale Factor: $SCALE_FACTOR"
echo "=========================================="

# Special handler for Person table (has Array columns)
load_person_table() {
    local dir_path="${DATA_BASE}/dynamic/Person"
    
    csv_files=$(ls "${dir_path}"/*.csv 2>/dev/null | grep -v '_SUCCESS' | sort)
    if [ -z "$csv_files" ]; then
        echo "  SKIP Person: No CSV files"
        return
    fi
    
    $CH_CLIENT --query="TRUNCATE TABLE IF EXISTS Person" 2>/dev/null || true
    
    # Load with transformation: split semicolon-separated strings into arrays
    # Concatenate all files, skip headers, and load in one command
    cd "${dir_path}"
    for csv_file in *.csv; do
        tail -n +2 "$csv_file" | sed 's/|/\t/g'
    done | $CH_CLIENT --query="INSERT INTO Person SELECT creationDate, id, firstName, lastName, gender, birthday, locationIP, browserUsed, splitByChar(';', language) AS speaks, splitByChar(';', email) AS email FROM input('creationDate Int64, id UInt64, firstName String, lastName String, gender String, birthday Int64, locationIP String, browserUsed String, language String, email String') FORMAT TabSeparated" 2>/dev/null || true
    
    count=$($CH_CLIENT --query="SELECT count() FROM Person" 2>/dev/null || echo "0")
    echo "  Person: ${count:-0} rows"
}

# Helper function to load CSV by converting to TSV
load_table() {
    local table=$1
    local subdir=$2
    local dir_path="${DATA_BASE}/${subdir}"
    
    # Find all CSV files
    csv_files=$(ls "${dir_path}"/*.csv 2>/dev/null | sort)
    if [ -z "$csv_files" ]; then
        echo "  SKIP $table: No CSV in ${subdir}"
        return
    fi
    
    # Truncate table first
    $CH_CLIENT --query="TRUNCATE TABLE IF EXISTS $table" 2>/dev/null || true
    
    # Load each CSV file, converting pipe to tab
    for csv_file in $csv_files; do
        cat "$csv_file" | sed 's/|/\t/g' | \
        $CH_CLIENT --query="INSERT INTO $table FORMAT TabSeparatedWithNames" 2>/dev/null || true
    done
    
    # Get count  
    count=$($CH_CLIENT --query="SELECT count() FROM $table" 2>/dev/null || echo "0")
    echo "  $table: ${count:-0} rows"
}

echo ""
echo "Loading static tables..."
load_table "Place" "static/Place"
load_table "TagClass" "static/TagClass" 
load_table "Tag" "static/Tag"
load_table "Organisation" "static/Organisation"
load_table "Place_isPartOf_Place" "static/Place_isPartOf_Place"
load_table "Organisation_isLocatedIn_Place" "static/Organisation_isLocatedIn_Place"
load_table "Tag_hasType_TagClass" "static/Tag_hasType_TagClass"
load_table "TagClass_isSubclassOf_TagClass" "static/TagClass_isSubclassOf_TagClass"

echo ""
echo "Loading dynamic tables..."
load_person_table  # Use special handler for Person (Array columns)
load_table "Forum" "dynamic/Forum"
load_table "Post" "dynamic/Post"
load_table "Comment" "dynamic/Comment"

echo ""
echo "Loading person relationships..."
load_table "Person_isLocatedIn_Place" "dynamic/Person_isLocatedIn_City"
load_table "Person_hasInterest_Tag" "dynamic/Person_hasInterest_Tag"
load_table "Person_studyAt_Organisation" "dynamic/Person_studyAt_University"
load_table "Person_workAt_Organisation" "dynamic/Person_workAt_Company"
load_table "Person_knows_Person" "dynamic/Person_knows_Person"
load_table "Person_likes_Post" "dynamic/Person_likes_Post"
load_table "Person_likes_Comment" "dynamic/Person_likes_Comment"

echo ""
echo "Loading forum relationships..."
load_table "Forum_hasModerator_Person" "dynamic/Forum_hasModerator_Person"
load_table "Forum_hasMember_Person" "dynamic/Forum_hasMember_Person"
load_table "Forum_hasTag_Tag" "dynamic/Forum_hasTag_Tag"
load_table "Forum_containerOf_Post" "dynamic/Forum_containerOf_Post"

echo ""
echo "Loading post relationships..."
load_table "Post_hasCreator_Person" "dynamic/Post_hasCreator_Person"
load_table "Post_isLocatedIn_Place" "dynamic/Post_isLocatedIn_Country"
load_table "Post_hasTag_Tag" "dynamic/Post_hasTag_Tag"

echo ""
echo "Loading comment relationships..."
load_table "Comment_hasCreator_Person" "dynamic/Comment_hasCreator_Person"
load_table "Comment_isLocatedIn_Place" "dynamic/Comment_isLocatedIn_Country"
load_table "Comment_hasTag_Tag" "dynamic/Comment_hasTag_Tag"
load_table "Comment_replyOf_Post" "dynamic/Comment_replyOf_Post"
load_table "Comment_replyOf_Comment" "dynamic/Comment_replyOf_Comment"

echo ""
echo "=========================================="
echo "Load complete!"
echo "=========================================="
echo ""
echo "Summary:"
$CH_CLIENT --query="
SELECT 'Person' AS entity, count() AS cnt FROM Person 
UNION ALL SELECT 'Post', count() FROM Post
UNION ALL SELECT 'Comment', count() FROM Comment
UNION ALL SELECT 'Forum', count() FROM Forum
UNION ALL SELECT 'Person_knows_Person', count() FROM Person_knows_Person
UNION ALL SELECT 'Tag', count() FROM Tag
UNION ALL SELECT 'Place', count() FROM Place
"
