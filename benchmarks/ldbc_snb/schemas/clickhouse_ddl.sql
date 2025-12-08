-- LDBC SNB Interactive v1 Schema for ClickHouse
-- Based on the CsvBasic serializer format with LongDateFormatter

-- Create database
CREATE DATABASE IF NOT EXISTS ldbc;

-- =============================================================================
-- STATIC TABLES (reference data that doesn't change)
-- =============================================================================

-- Place hierarchy: Continent -> Country -> City
CREATE TABLE IF NOT EXISTS ldbc.Place (
    id UInt64,
    name String,
    url String,
    type String  -- 'Continent', 'Country', 'City'
) ENGINE = MergeTree()
ORDER BY id;

-- Place hierarchy relationship
CREATE TABLE IF NOT EXISTS ldbc.Place_isPartOf_Place (
    Place_id UInt64,
    Place_id_2 UInt64
) ENGINE = MergeTree()
ORDER BY (Place_id, Place_id_2);

-- Organisation: University or Company
CREATE TABLE IF NOT EXISTS ldbc.Organisation (
    id UInt64,
    name String,
    url String,
    type String  -- 'University', 'Company'
) ENGINE = MergeTree()
ORDER BY id;

-- Organisation location
CREATE TABLE IF NOT EXISTS ldbc.Organisation_isLocatedIn_Place (
    Organisation_id UInt64,
    Place_id UInt64
) ENGINE = MergeTree()
ORDER BY (Organisation_id, Place_id);

-- Tag
CREATE TABLE IF NOT EXISTS ldbc.Tag (
    id UInt64,
    name String,
    url String
) ENGINE = MergeTree()
ORDER BY id;

-- TagClass
CREATE TABLE IF NOT EXISTS ldbc.TagClass (
    id UInt64,
    name String,
    url String
) ENGINE = MergeTree()
ORDER BY id;

-- Tag -> TagClass
CREATE TABLE IF NOT EXISTS ldbc.Tag_hasType_TagClass (
    Tag_id UInt64,
    TagClass_id UInt64
) ENGINE = MergeTree()
ORDER BY (Tag_id, TagClass_id);

-- TagClass hierarchy
CREATE TABLE IF NOT EXISTS ldbc.TagClass_isSubclassOf_TagClass (
    TagClass_id UInt64,
    TagClass_id_2 UInt64
) ENGINE = MergeTree()
ORDER BY (TagClass_id, TagClass_id_2);

-- =============================================================================
-- DYNAMIC TABLES (social network data)
-- =============================================================================

-- Person
CREATE TABLE IF NOT EXISTS ldbc.Person (
    id UInt64,
    firstName String,
    lastName String,
    gender String,
    birthday Int64,  -- epoch milliseconds
    creationDate Int64,  -- epoch milliseconds
    locationIP String,
    browserUsed String,
    speaks Array(String),
    email Array(String)
) ENGINE = MergeTree()
ORDER BY id;

-- Person location
CREATE TABLE IF NOT EXISTS ldbc.Person_isLocatedIn_Place (
    Person_id UInt64,
    Place_id UInt64
) ENGINE = MergeTree()
ORDER BY (Person_id, Place_id);

-- Person interests
CREATE TABLE IF NOT EXISTS ldbc.Person_hasInterest_Tag (
    Person_id UInt64,
    Tag_id UInt64
) ENGINE = MergeTree()
ORDER BY (Person_id, Tag_id);

-- Person work
CREATE TABLE IF NOT EXISTS ldbc.Person_workAt_Organisation (
    Person_id UInt64,
    Organisation_id UInt64,
    workFrom Int32
) ENGINE = MergeTree()
ORDER BY (Person_id, Organisation_id);

-- Person study
CREATE TABLE IF NOT EXISTS ldbc.Person_studyAt_Organisation (
    Person_id UInt64,
    Organisation_id UInt64,
    classYear Int32
) ENGINE = MergeTree()
ORDER BY (Person_id, Organisation_id);

-- Person knows Person (bidirectional friendship)
CREATE TABLE IF NOT EXISTS ldbc.Person_knows_Person (
    Person_id UInt64,
    Person_id_2 UInt64,
    creationDate Int64  -- epoch milliseconds
) ENGINE = MergeTree()
ORDER BY (Person_id, Person_id_2);

-- Forum
CREATE TABLE IF NOT EXISTS ldbc.Forum (
    id UInt64,
    title String,
    creationDate Int64  -- epoch milliseconds
) ENGINE = MergeTree()
ORDER BY id;

-- Forum moderator
CREATE TABLE IF NOT EXISTS ldbc.Forum_hasModerator_Person (
    Forum_id UInt64,
    Person_id UInt64
) ENGINE = MergeTree()
ORDER BY (Forum_id, Person_id);

-- Forum members
CREATE TABLE IF NOT EXISTS ldbc.Forum_hasMember_Person (
    Forum_id UInt64,
    Person_id UInt64,
    joinDate Int64  -- epoch milliseconds
) ENGINE = MergeTree()
ORDER BY (Forum_id, Person_id);

-- Forum tags
CREATE TABLE IF NOT EXISTS ldbc.Forum_hasTag_Tag (
    Forum_id UInt64,
    Tag_id UInt64
) ENGINE = MergeTree()
ORDER BY (Forum_id, Tag_id);

-- Post
CREATE TABLE IF NOT EXISTS ldbc.Post (
    id UInt64,
    imageFile String,
    creationDate Int64,  -- epoch milliseconds
    locationIP String,
    browserUsed String,
    language String,
    content String,
    length UInt32
) ENGINE = MergeTree()
ORDER BY id;

-- Post creator
CREATE TABLE IF NOT EXISTS ldbc.Post_hasCreator_Person (
    Post_id UInt64,
    Person_id UInt64
) ENGINE = MergeTree()
ORDER BY (Post_id, Person_id);

-- Post location
CREATE TABLE IF NOT EXISTS ldbc.Post_isLocatedIn_Place (
    Post_id UInt64,
    Place_id UInt64
) ENGINE = MergeTree()
ORDER BY (Post_id, Place_id);

-- Post tags
CREATE TABLE IF NOT EXISTS ldbc.Post_hasTag_Tag (
    Post_id UInt64,
    Tag_id UInt64
) ENGINE = MergeTree()
ORDER BY (Post_id, Tag_id);

-- Forum contains Post
CREATE TABLE IF NOT EXISTS ldbc.Forum_containerOf_Post (
    Forum_id UInt64,
    Post_id UInt64
) ENGINE = MergeTree()
ORDER BY (Forum_id, Post_id);

-- Comment
CREATE TABLE IF NOT EXISTS ldbc.Comment (
    id UInt64,
    creationDate Int64,  -- epoch milliseconds
    locationIP String,
    browserUsed String,
    content String,
    length UInt32
) ENGINE = MergeTree()
ORDER BY id;

-- Comment creator
CREATE TABLE IF NOT EXISTS ldbc.Comment_hasCreator_Person (
    Comment_id UInt64,
    Person_id UInt64
) ENGINE = MergeTree()
ORDER BY (Comment_id, Person_id);

-- Comment location
CREATE TABLE IF NOT EXISTS ldbc.Comment_isLocatedIn_Place (
    Comment_id UInt64,
    Place_id UInt64
) ENGINE = MergeTree()
ORDER BY (Comment_id, Place_id);

-- Comment tags
CREATE TABLE IF NOT EXISTS ldbc.Comment_hasTag_Tag (
    Comment_id UInt64,
    Tag_id UInt64
) ENGINE = MergeTree()
ORDER BY (Comment_id, Tag_id);

-- Comment replies to Post
CREATE TABLE IF NOT EXISTS ldbc.Comment_replyOf_Post (
    Comment_id UInt64,
    Post_id UInt64
) ENGINE = MergeTree()
ORDER BY (Comment_id, Post_id);

-- Comment replies to Comment
CREATE TABLE IF NOT EXISTS ldbc.Comment_replyOf_Comment (
    Comment_id UInt64,
    Comment_id_2 UInt64
) ENGINE = MergeTree()
ORDER BY (Comment_id, Comment_id_2);

-- Person likes Post
CREATE TABLE IF NOT EXISTS ldbc.Person_likes_Post (
    Person_id UInt64,
    Post_id UInt64,
    creationDate Int64  -- epoch milliseconds
) ENGINE = MergeTree()
ORDER BY (Person_id, Post_id);

-- Person likes Comment
CREATE TABLE IF NOT EXISTS ldbc.Person_likes_Comment (
    Person_id UInt64,
    Comment_id UInt64,
    creationDate Int64  -- epoch milliseconds
) ENGINE = MergeTree()
ORDER BY (Person_id, Comment_id);

-- =============================================================================
-- VIEWS for unified Message type (Post + Comment)
-- =============================================================================

-- Unified Message view (Post and Comment have similar structure)
CREATE VIEW IF NOT EXISTS ldbc.Message AS
SELECT 
    id,
    creationDate,
    locationIP,
    browserUsed,
    content,
    length,
    imageFile,
    language,
    'Post' AS type
FROM ldbc.Post
UNION ALL
SELECT 
    id,
    creationDate,
    locationIP,
    browserUsed,
    content,
    length,
    '' AS imageFile,
    '' AS language,
    'Comment' AS type
FROM ldbc.Comment;

-- Unified Message creator view
CREATE VIEW IF NOT EXISTS ldbc.Message_hasCreator_Person AS
SELECT Post_id AS Message_id, Person_id FROM ldbc.Post_hasCreator_Person
UNION ALL
SELECT Comment_id AS Message_id, Person_id FROM ldbc.Comment_hasCreator_Person;

-- Unified likes view
CREATE VIEW IF NOT EXISTS ldbc.Person_likes_Message AS
SELECT Person_id, Post_id AS Message_id, creationDate FROM ldbc.Person_likes_Post
UNION ALL
SELECT Person_id, Comment_id AS Message_id, creationDate FROM ldbc.Person_likes_Comment;
