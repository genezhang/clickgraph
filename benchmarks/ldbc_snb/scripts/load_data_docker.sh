#!/bin/bash
# Load LDBC SNB data via Docker
# Usage: ./load_data_docker.sh sf10

SCALE_FACTOR=${1:-sf0.003}
DATA_BASE="/data/${SCALE_FACTOR}/graphs/csv/interactive/composite-projected-fk"
DATABASE="ldbc"
USER="default"
PASSWORD="default"

echo "=========================================="
echo "LDBC SNB Data Loader (Docker)"
echo "=========================================="
echo "Scale Factor: $SCALE_FACTOR"
echo "Data Path: $DATA_BASE"
echo "=========================================="

# Helper function to load a CSV file
load_table() {
    local table=$1
    local subdir=$2
    local dir_path="${DATA_BASE}/${subdir}"
    
    # Find CSV file in directory
    csv_file=$(ls "${dir_path}"/*.csv 2>/dev/null | head -1)
    if [ -z "$csv_file" ]; then
        echo "  SKIP: No CSV in ${subdir}"
        return
    fi
    
    # Truncate table first
    clickhouse-client --user="$USER" --password="$PASSWORD" --database="$DATABASE" \
        --query="TRUNCATE TABLE IF EXISTS $table"
    
    # Load data
    clickhouse-client --user="$USER" --password="$PASSWORD" --database="$DATABASE" \
        --query="INSERT INTO $table FORMAT CSVWithNames SETTINGS format_csv_delimiter='|'" \
        < "$csv_file"
    
    # Get count
    count=$(clickhouse-client --user="$USER" --password="$PASSWORD" --database="$DATABASE" \
        --query="SELECT count() FROM $table")
    
    echo "  $table: $count rows"
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
load_table "Person" "dynamic/Person"
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
clickhouse-client --user="$USER" --password="$PASSWORD" --database="$DATABASE" \
    --query="SELECT 'Person' AS entity, count() AS cnt FROM Person 
             UNION ALL SELECT 'Post', count() FROM Post
             UNION ALL SELECT 'Comment', count() FROM Comment
             UNION ALL SELECT 'Forum', count() FROM Forum
             UNION ALL SELECT 'Person_knows_Person', count() FROM Person_knows_Person"
