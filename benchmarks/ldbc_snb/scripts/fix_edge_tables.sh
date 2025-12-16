#!/bin/bash
# Fix all edge tables by reloading with correct column mapping
# This script truncates and reloads edge tables that had column name mismatches

set -e

DATABASE="ldbc"
USER="default"
PASSWORD="default"
DATA_BASE="/data/sf10/graphs/csv/interactive/composite-projected-fk"

CH_CLIENT="clickhouse-client --user=$USER --password=$PASSWORD --database=$DATABASE"

echo "=========================================="
echo "LDBC Edge Table Fix - SF10"
echo "=========================================="
echo ""

# Person_hasInterest_Tag: CSV has creationDate|PersonId|TagId
echo "Fixing Person_hasInterest_Tag..."
$CH_CLIENT --query="TRUNCATE TABLE Person_hasInterest_Tag"
cd ${DATA_BASE}/dynamic/Person_hasInterest_Tag
for f in *.csv; do
  tail -n +2 "$f" | sed 's/|/\t/g'
done | $CH_CLIENT --query="INSERT INTO Person_hasInterest_Tag (Person_id, Tag_id) SELECT column2, column3 FROM input('column1 Int64, column2 UInt64, column3 UInt64') FORMAT TabSeparated"
count=$($CH_CLIENT --query="SELECT count() FROM Person_hasInterest_Tag")
echo "  ✓ Person_hasInterest_Tag: $count rows"

# Person_studyAt_Organisation: CSV has creationDate|PersonId|UniversityId|classYear
echo "Fixing Person_studyAt_Organisation..."
$CH_CLIENT --query="TRUNCATE TABLE Person_studyAt_Organisation"
cd ${DATA_BASE}/dynamic/Person_studyAt_University
for f in *.csv; do
  tail -n +2 "$f" | sed 's/|/\t/g'
done | $CH_CLIENT --query="INSERT INTO Person_studyAt_Organisation (Person_id, Organisation_id, classYear) SELECT column2, column3, column4 FROM input('column1 Int64, column2 UInt64, column3 UInt64, column4 Int32') FORMAT TabSeparated"
count=$($CH_CLIENT --query="SELECT count() FROM Person_studyAt_Organisation")
echo "  ✓ Person_studyAt_Organisation: $count rows"

# Person_workAt_Organisation: CSV has creationDate|PersonId|CompanyId|workFrom
echo "Fixing Person_workAt_Organisation..."
$CH_CLIENT --query="TRUNCATE TABLE Person_workAt_Organisation"
cd ${DATA_BASE}/dynamic/Person_workAt_Company
for f in *.csv; do
  tail -n +2 "$f" | sed 's/|/\t/g'
done | $CH_CLIENT --query="INSERT INTO Person_workAt_Organisation (Person_id, Organisation_id, workFrom) SELECT column2, column3, column4 FROM input('column1 Int64, column2 UInt64, column3 UInt64, column4 Int32') FORMAT TabSeparated"
count=$($CH_CLIENT --query="SELECT count() FROM Person_workAt_Organisation")
echo "  ✓ Person_workAt_Organisation: $count rows"

# Person_likes_Post: CSV has creationDate|PersonId|PostId
echo "Fixing Person_likes_Post..."
$CH_CLIENT --query="TRUNCATE TABLE Person_likes_Post"
cd ${DATA_BASE}/dynamic/Person_likes_Post
for f in *.csv; do
  tail -n +2 "$f" | sed 's/|/\t/g'
done | $CH_CLIENT --query="INSERT INTO Person_likes_Post (Person_id, Post_id, creationDate) SELECT column2, column3, column1 FROM input('column1 Int64, column2 UInt64, column3 UInt64') FORMAT TabSeparated"
count=$($CH_CLIENT --query="SELECT count() FROM Person_likes_Post")
echo "  ✓ Person_likes_Post: $count rows"

# Person_likes_Comment: CSV has creationDate|PersonId|CommentId
echo "Fixing Person_likes_Comment..."
$CH_CLIENT --query="TRUNCATE TABLE Person_likes_Comment"
cd ${DATA_BASE}/dynamic/Person_likes_Comment
for f in *.csv; do
  tail -n +2 "$f" | sed 's/|/\t/g'
done | $CH_CLIENT --query="INSERT INTO Person_likes_Comment (Person_id, Comment_id, creationDate) SELECT column2, column3, column1 FROM input('column1 Int64, column2 UInt64, column3 UInt64') FORMAT TabSeparated"
count=$($CH_CLIENT --query="SELECT count() FROM Person_likes_Comment")
echo "  ✓ Person_likes_Comment: $count rows"

# Organisation_isLocatedIn_Place: CSV has OrganisationId|PlaceId
echo "Fixing Organisation_isLocatedIn_Place..."
$CH_CLIENT --query="TRUNCATE TABLE Organisation_isLocatedIn_Place"
cd ${DATA_BASE}/static/Organisation_isLocatedIn_Place
for f in *.csv; do
  tail -n +2 "$f" | sed 's/|/\t/g'
done | $CH_CLIENT --query="INSERT INTO Organisation_isLocatedIn_Place (Organisation_id, Place_id) SELECT column1, column2 FROM input('column1 UInt64, column2 UInt64') FORMAT TabSeparated"
count=$($CH_CLIENT --query="SELECT count() FROM Organisation_isLocatedIn_Place")
echo "  ✓ Organisation_isLocatedIn_Place: $count rows"

# Post_isLocatedIn_Place: CSV has creationDate|PostId|CountryId
echo "Fixing Post_isLocatedIn_Place..."
$CH_CLIENT --query="TRUNCATE TABLE Post_isLocatedIn_Place"
cd ${DATA_BASE}/dynamic/Post_isLocatedIn_Country
for f in *.csv; do
  tail -n +2 "$f" | sed 's/|/\t/g'
done | $CH_CLIENT --query="INSERT INTO Post_isLocatedIn_Place (Post_id, Place_id) SELECT column2, column3 FROM input('column1 Int64, column2 UInt64, column3 UInt64') FORMAT TabSeparated"
count=$($CH_CLIENT --query="SELECT count() FROM Post_isLocatedIn_Place")
echo "  ✓ Post_isLocatedIn_Place: $count rows"

# Comment_isLocatedIn_Place: CSV has creationDate|CommentId|CountryId
echo "Fixing Comment_isLocatedIn_Place..."
$CH_CLIENT --query="TRUNCATE TABLE Comment_isLocatedIn_Place"
cd ${DATA_BASE}/dynamic/Comment_isLocatedIn_Country
for f in *.csv; do
  tail -n +2 "$f" | sed 's/|/\t/g'
done | $CH_CLIENT --query="INSERT INTO Comment_isLocatedIn_Place (Comment_id, Place_id) SELECT column2, column3 FROM input('column1 Int64, column2 UInt64, column3 UInt64') FORMAT TabSeparated"
count=$($CH_CLIENT --query="SELECT count() FROM Comment_isLocatedIn_Place")
echo "  ✓ Comment_isLocatedIn_Place: $count rows"

echo ""
echo "=========================================="
echo "All edge tables fixed!"
echo "=========================================="
echo ""
echo "Final counts:"
$CH_CLIENT --query="
SELECT 'Person_hasInterest_Tag' AS table, count() AS cnt FROM Person_hasInterest_Tag
UNION ALL SELECT 'Person_studyAt_Organisation', count() FROM Person_studyAt_Organisation
UNION ALL SELECT 'Person_workAt_Organisation', count() FROM Person_workAt_Organisation
UNION ALL SELECT 'Person_likes_Post', count() FROM Person_likes_Post
UNION ALL SELECT 'Person_likes_Comment', count() FROM Person_likes_Comment
UNION ALL SELECT 'Person_isLocatedIn_Place', count() FROM Person_isLocatedIn_Place
UNION ALL SELECT 'Organisation_isLocatedIn_Place', count() FROM Organisation_isLocatedIn_Place
UNION ALL SELECT 'Post_isLocatedIn_Place', count() FROM Post_isLocatedIn_Place
UNION ALL SELECT 'Comment_isLocatedIn_Place', count() FROM Comment_isLocatedIn_Place
FORMAT PrettyCompact
"
