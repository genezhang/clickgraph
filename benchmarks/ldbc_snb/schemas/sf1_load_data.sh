#!/bin/bash
# sf1 Data Loading Script
#
# Creates tables and loads LDBC SNB sf1 CsvBasic v1 data into ClickHouse.
# Run sf1_ddl_load.sql first (DDL), then this script (data loading).
#
# Usage:
#   bash sf1_load_data.sh [CH_URL] [CH_USER] [CH_PASS] [DATA_DIR]
#
# Defaults:
#   CH_URL=http://localhost:18123  CH_USER=test_user  CH_PASS=test_pass
#   DATA_DIR=/data/sf1  (path inside ClickHouse container)

set -euo pipefail

CH_URL="${1:-http://localhost:18123}"
CH_USER="${2:-test_user}"
CH_PASS="${3:-test_pass}"
DATA_DIR="${4:-/data/sf1}"

ch() {
    curl -sS "${CH_URL}/?user=${CH_USER}&password=${CH_PASS}" --data-binary "$1"
}

echo "=== Loading sf1 data from ${DATA_DIR} ==="

# ============================================================
# Static node tables (no creationDate)
# ============================================================

echo "Loading Place..."
ch "INSERT INTO ldbc.Place SELECT
    c1 AS id, c2 AS name, c3 AS url, c4 AS type
FROM file('${DATA_DIR}/static/place_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 String, c3 String, c4 String')
SETTINGS format_csv_delimiter='|'"

echo "Loading Organisation..."
ch "INSERT INTO ldbc.Organisation SELECT
    c1 AS id, c2 AS type, c3 AS name, c4 AS url
FROM file('${DATA_DIR}/static/organisation_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 String, c3 String, c4 String')
SETTINGS format_csv_delimiter='|'"

echo "Loading Tag..."
ch "INSERT INTO ldbc.Tag SELECT
    c1 AS id, c2 AS name, c3 AS url
FROM file('${DATA_DIR}/static/tag_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 String, c3 String')
SETTINGS format_csv_delimiter='|'"

echo "Loading TagClass..."
ch "INSERT INTO ldbc.TagClass SELECT
    c1 AS id, c2 AS name, c3 AS url
FROM file('${DATA_DIR}/static/tagclass_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 String, c3 String')
SETTINGS format_csv_delimiter='|'"

# ============================================================
# Static edge tables (no creationDate)
# ============================================================

echo "Loading Organisation_isLocatedIn_Place..."
ch "INSERT INTO ldbc.Organisation_isLocatedIn_Place SELECT
    c1 AS OrganisationId, c2 AS PlaceId
FROM file('${DATA_DIR}/static/organisation_isLocatedIn_place_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Place_isPartOf_Place..."
ch "INSERT INTO ldbc.Place_isPartOf_Place SELECT
    c1 AS Place1Id, c2 AS Place2Id
FROM file('${DATA_DIR}/static/place_isPartOf_place_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Tag_hasType_TagClass..."
ch "INSERT INTO ldbc.Tag_hasType_TagClass SELECT
    c1 AS TagId, c2 AS TagClassId
FROM file('${DATA_DIR}/static/tag_hasType_tagclass_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

echo "Loading TagClass_isSubclassOf_TagClass..."
ch "INSERT INTO ldbc.TagClass_isSubclassOf_TagClass SELECT
    c1 AS TagClass1Id, c2 AS TagClass2Id
FROM file('${DATA_DIR}/static/tagclass_isSubclassOf_tagclass_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

# ============================================================
# Dynamic node tables
# ============================================================

# Person: load base + speaks/email via staging tables, then aggregate
echo "Loading Person (step 1: staging tables)..."

ch "CREATE TABLE IF NOT EXISTS ldbc._person_base
(creationDate Int64, id UInt64, firstName String, lastName String,
 gender String, birthday Int64, locationIP String, browserUsed String)
ENGINE = Memory"

ch "CREATE TABLE IF NOT EXISTS ldbc._person_speaks
(PersonId UInt64, language String) ENGINE = Memory"

ch "CREATE TABLE IF NOT EXISTS ldbc._person_email
(PersonId UInt64, email String) ENGINE = Memory"

ch "INSERT INTO ldbc._person_base SELECT
    c6 AS creationDate, c1 AS id, c2 AS firstName, c3 AS lastName,
    c4 AS gender, c5 AS birthday, c7 AS locationIP, c8 AS browserUsed
FROM file('${DATA_DIR}/dynamic/person_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 String, c3 String, c4 String, c5 Int64, c6 Int64, c7 String, c8 String')
SETTINGS format_csv_delimiter='|'"

ch "INSERT INTO ldbc._person_speaks SELECT c1, c2
FROM file('${DATA_DIR}/dynamic/person_speaks_language_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 String')
SETTINGS format_csv_delimiter='|'"

ch "INSERT INTO ldbc._person_email SELECT c1, c2
FROM file('${DATA_DIR}/dynamic/person_email_emailaddress_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 String')
SETTINGS format_csv_delimiter='|'"

echo "Loading Person (step 2: aggregate into Person table)..."
ch "INSERT INTO ldbc.Person
SELECT
    p.creationDate, p.id, p.firstName, p.lastName, p.gender, p.birthday,
    p.locationIP, p.browserUsed,
    ifNull(s.speaks, []) AS speaks,
    ifNull(e.email, []) AS email
FROM ldbc._person_base AS p
LEFT JOIN (SELECT PersonId, groupArray(language) AS speaks FROM ldbc._person_speaks GROUP BY PersonId) AS s
    ON p.id = s.PersonId
LEFT JOIN (SELECT PersonId, groupArray(email) AS email FROM ldbc._person_email GROUP BY PersonId) AS e
    ON p.id = e.PersonId"

ch "DROP TABLE IF EXISTS ldbc._person_base"
ch "DROP TABLE IF EXISTS ldbc._person_speaks"
ch "DROP TABLE IF EXISTS ldbc._person_email"

echo "Loading Comment..."
ch "INSERT INTO ldbc.Comment SELECT
    c2 AS creationDate, c1 AS id, c3 AS locationIP, c4 AS browserUsed, c5 AS content, c6 AS length
FROM file('${DATA_DIR}/dynamic/comment_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 Int64, c3 String, c4 String, c5 String, c6 UInt32')
SETTINGS format_csv_delimiter='|'"

echo "Loading Post..."
ch "INSERT INTO ldbc.Post SELECT
    c3 AS creationDate, c1 AS id, c2 AS imageFile, c4 AS locationIP, c5 AS browserUsed,
    c6 AS language, c7 AS content, c8 AS length
FROM file('${DATA_DIR}/dynamic/post_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 String, c3 Int64, c4 String, c5 String, c6 String, c7 String, c8 UInt32')
SETTINGS format_csv_delimiter='|'"

echo "Loading Forum..."
ch "INSERT INTO ldbc.Forum SELECT
    c3 AS creationDate, c1 AS id, c2 AS title
FROM file('${DATA_DIR}/dynamic/forum_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 String, c3 Int64')
SETTINGS format_csv_delimiter='|'"

# ============================================================
# Dynamic edge tables
# ============================================================

echo "Loading Person_knows_Person..."
ch "INSERT INTO ldbc.Person_knows_Person SELECT
    c3 AS creationDate, c1 AS Person1Id, c2 AS Person2Id
FROM file('${DATA_DIR}/dynamic/person_knows_person_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64, c3 Int64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Person_studyAt_Organisation..."
ch "INSERT INTO ldbc.Person_studyAt_Organisation SELECT
    0 AS creationDate, c1 AS PersonId, c2 AS UniversityId, c3 AS classYear
FROM file('${DATA_DIR}/dynamic/person_studyAt_organisation_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64, c3 Int32')
SETTINGS format_csv_delimiter='|'"

echo "Loading Person_workAt_Organisation..."
ch "INSERT INTO ldbc.Person_workAt_Organisation SELECT
    0 AS creationDate, c1 AS PersonId, c2 AS CompanyId, c3 AS workFrom
FROM file('${DATA_DIR}/dynamic/person_workAt_organisation_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64, c3 Int32')
SETTINGS format_csv_delimiter='|'"

echo "Loading Person_hasInterest_Tag..."
ch "INSERT INTO ldbc.Person_hasInterest_Tag SELECT
    0 AS creationDate, c1 AS PersonId, c2 AS TagId
FROM file('${DATA_DIR}/dynamic/person_hasInterest_tag_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Person_isLocatedIn_Place..."
ch "INSERT INTO ldbc.Person_isLocatedIn_Place SELECT
    0 AS creationDate, c1 AS PersonId, c2 AS CityId
FROM file('${DATA_DIR}/dynamic/person_isLocatedIn_place_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Person_likes_Comment..."
ch "INSERT INTO ldbc.Person_likes_Comment SELECT
    c3 AS creationDate, c1 AS PersonId, c2 AS CommentId
FROM file('${DATA_DIR}/dynamic/person_likes_comment_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64, c3 Int64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Person_likes_Post..."
ch "INSERT INTO ldbc.Person_likes_Post SELECT
    c3 AS creationDate, c1 AS PersonId, c2 AS PostId
FROM file('${DATA_DIR}/dynamic/person_likes_post_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64, c3 Int64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Comment_hasCreator_Person..."
ch "INSERT INTO ldbc.Comment_hasCreator_Person SELECT
    0 AS creationDate, c1 AS CommentId, c2 AS PersonId
FROM file('${DATA_DIR}/dynamic/comment_hasCreator_person_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Comment_hasTag_Tag..."
ch "INSERT INTO ldbc.Comment_hasTag_Tag SELECT
    0 AS creationDate, c1 AS CommentId, c2 AS TagId
FROM file('${DATA_DIR}/dynamic/comment_hasTag_tag_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Comment_isLocatedIn_Place..."
ch "INSERT INTO ldbc.Comment_isLocatedIn_Place SELECT
    0 AS creationDate, c1 AS CommentId, c2 AS CountryId
FROM file('${DATA_DIR}/dynamic/comment_isLocatedIn_place_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Comment_replyOf_Comment..."
ch "INSERT INTO ldbc.Comment_replyOf_Comment SELECT
    0 AS creationDate, c1 AS Comment1Id, c2 AS Comment2Id
FROM file('${DATA_DIR}/dynamic/comment_replyOf_comment_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Comment_replyOf_Post..."
ch "INSERT INTO ldbc.Comment_replyOf_Post SELECT
    0 AS creationDate, c1 AS CommentId, c2 AS PostId
FROM file('${DATA_DIR}/dynamic/comment_replyOf_post_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Post_hasCreator_Person..."
ch "INSERT INTO ldbc.Post_hasCreator_Person SELECT
    0 AS creationDate, c1 AS PostId, c2 AS PersonId
FROM file('${DATA_DIR}/dynamic/post_hasCreator_person_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Post_hasTag_Tag..."
ch "INSERT INTO ldbc.Post_hasTag_Tag SELECT
    0 AS creationDate, c1 AS PostId, c2 AS TagId
FROM file('${DATA_DIR}/dynamic/post_hasTag_tag_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Post_isLocatedIn_Place..."
ch "INSERT INTO ldbc.Post_isLocatedIn_Place SELECT
    0 AS creationDate, c1 AS PostId, c2 AS CountryId
FROM file('${DATA_DIR}/dynamic/post_isLocatedIn_place_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Forum_containerOf_Post..."
ch "INSERT INTO ldbc.Forum_containerOf_Post SELECT
    0 AS creationDate, c1 AS ForumId, c2 AS PostId
FROM file('${DATA_DIR}/dynamic/forum_containerOf_post_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Forum_hasMember_Person..."
ch "INSERT INTO ldbc.Forum_hasMember_Person SELECT
    c3 AS creationDate, c1 AS ForumId, c2 AS PersonId
FROM file('${DATA_DIR}/dynamic/forum_hasMember_person_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64, c3 Int64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Forum_hasModerator_Person..."
ch "INSERT INTO ldbc.Forum_hasModerator_Person SELECT
    0 AS creationDate, c1 AS ForumId, c2 AS PersonId
FROM file('${DATA_DIR}/dynamic/forum_hasModerator_person_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

echo "Loading Forum_hasTag_Tag..."
ch "INSERT INTO ldbc.Forum_hasTag_Tag SELECT
    0 AS creationDate, c1 AS ForumId, c2 AS TagId
FROM file('${DATA_DIR}/dynamic/forum_hasTag_tag_0_0.csv', 'CSVWithNames',
    'c1 UInt64, c2 UInt64')
SETTINGS format_csv_delimiter='|'"

# ============================================================
# 4. UNION VIEWS (Message = Post + Comment)
# ============================================================

echo "Creating union views..."

ch "CREATE VIEW IF NOT EXISTS ldbc.Message AS
SELECT id, creationDate, locationIP, browserUsed, content, length, imageFile, language, 'Post' AS type
FROM ldbc.Post
UNION ALL
SELECT id, creationDate, locationIP, browserUsed, content, length, '' AS imageFile, '' AS language, 'Comment' AS type
FROM ldbc.Comment"

ch "CREATE VIEW IF NOT EXISTS ldbc.Message_hasCreator_Person AS
SELECT PostId AS MessageId, PersonId, creationDate
FROM ldbc.Post_hasCreator_Person
UNION ALL
SELECT CommentId AS MessageId, PersonId, creationDate
FROM ldbc.Comment_hasCreator_Person"

ch "CREATE VIEW IF NOT EXISTS ldbc.Message_hasTag_Tag AS
SELECT PostId AS MessageId, TagId, creationDate
FROM ldbc.Post_hasTag_Tag
UNION ALL
SELECT CommentId AS MessageId, TagId, creationDate
FROM ldbc.Comment_hasTag_Tag"

ch "CREATE VIEW IF NOT EXISTS ldbc.Message_isLocatedIn_Place AS
SELECT creationDate, PostId AS MessageId, CountryId AS PlaceId
FROM ldbc.Post_isLocatedIn_Place
UNION ALL
SELECT creationDate, CommentId AS MessageId, CountryId AS PlaceId
FROM ldbc.Comment_isLocatedIn_Place"

ch "CREATE VIEW IF NOT EXISTS ldbc.Message_replyOf_Message AS
SELECT CommentId AS MessageId, PostId AS TargetMessageId, creationDate
FROM ldbc.Comment_replyOf_Post
UNION ALL
SELECT Comment1Id AS MessageId, Comment2Id AS TargetMessageId, creationDate
FROM ldbc.Comment_replyOf_Comment"

ch "CREATE VIEW IF NOT EXISTS ldbc.Person_likes_Message AS
SELECT creationDate, PersonId, PostId AS MessageId
FROM ldbc.Person_likes_Post
UNION ALL
SELECT creationDate, PersonId, CommentId AS MessageId
FROM ldbc.Person_likes_Comment"

ch "CREATE VIEW IF NOT EXISTS ldbc.Comment_replyOf_Message AS
SELECT creationDate, CommentId, PostId AS MessageId
FROM ldbc.Comment_replyOf_Post
UNION ALL
SELECT creationDate, Comment1Id AS CommentId, Comment2Id AS MessageId
FROM ldbc.Comment_replyOf_Comment"

# ============================================================
# 5. ALIAS COLUMNS (same as sf10_normalize.sql)
# ============================================================

echo "Adding ALIAS columns for normalization..."

ch "ALTER TABLE ldbc.Person_studyAt_Organisation ADD COLUMN IF NOT EXISTS OrganisationId UInt64 ALIAS UniversityId"
ch "ALTER TABLE ldbc.Person_workAt_Organisation ADD COLUMN IF NOT EXISTS OrganisationId UInt64 ALIAS CompanyId"
ch "ALTER TABLE ldbc.Post_isLocatedIn_Place ADD COLUMN IF NOT EXISTS PlaceId UInt64 ALIAS CountryId"
ch "ALTER TABLE ldbc.Comment_isLocatedIn_Place ADD COLUMN IF NOT EXISTS PlaceId UInt64 ALIAS CountryId"

# ============================================================
# 6. VERIFY ROW COUNTS
# ============================================================

echo ""
echo "=== Row counts ==="
for t in Person Comment Post Forum Organisation Place Tag TagClass \
         Person_knows_Person Person_studyAt_Organisation Person_workAt_Organisation \
         Person_hasInterest_Tag Person_isLocatedIn_Place Person_likes_Comment \
         Person_likes_Post Comment_hasCreator_Person Comment_hasTag_Tag \
         Comment_isLocatedIn_Place Comment_replyOf_Comment Comment_replyOf_Post \
         Post_hasCreator_Person Post_hasTag_Tag Post_isLocatedIn_Place \
         Forum_containerOf_Post Forum_hasMember_Person Forum_hasModerator_Person \
         Forum_hasTag_Tag Organisation_isLocatedIn_Place Place_isPartOf_Place \
         Tag_hasType_TagClass TagClass_isSubclassOf_TagClass; do
    count=$(ch "SELECT count() FROM ldbc.$t")
    printf "%-40s %s\n" "$t" "$count"
done

echo ""
echo "=== Done! ==="
