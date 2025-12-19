-- LDBC SNB Interactive v1 Schema for ClickHouse
-- CORRECTED VERSION - Column names match actual CSV headers exactly
-- Generated: December 15, 2025

-- Create database
CREATE DATABASE IF NOT EXISTS ldbc;

-- =============================================================================
-- STATIC TABLES (reference data that doesn't change)
-- =============================================================================

-- Place hierarchy: Continent -> Country -> City
-- CSV: id|name|url|type
CREATE TABLE IF NOT EXISTS ldbc.Place (
    id UInt64,
    name String,
    url String,
    type String  -- 'Continent', 'Country', 'City'
) ENGINE = MergeTree()
ORDER BY id;

-- Place hierarchy relationship
-- CSV: Place1Id|Place2Id
CREATE TABLE IF NOT EXISTS ldbc.Place_isPartOf_Place (
    Place1Id UInt64,
    Place2Id UInt64
) ENGINE = MergeTree()
ORDER BY (Place1Id, Place2Id);

-- Organisation: University or Company
-- CSV: id|type|name|url
CREATE TABLE IF NOT EXISTS ldbc.Organisation (
    id UInt64,
    type String,  -- 'University', 'Company'
    name String,
    url String
) ENGINE = MergeTree()
ORDER BY id;

-- Organisation location
-- CSV: OrganisationId|PlaceId
CREATE TABLE IF NOT EXISTS ldbc.Organisation_isLocatedIn_Place (
    OrganisationId UInt64,
    PlaceId UInt64
) ENGINE = MergeTree()
ORDER BY (OrganisationId, PlaceId);

-- Tag
-- CSV: id|name|url
CREATE TABLE IF NOT EXISTS ldbc.Tag (
    id UInt64,
    name String,
    url String
) ENGINE = MergeTree()
ORDER BY id;

-- TagClass
-- CSV: id|name|url
CREATE TABLE IF NOT EXISTS ldbc.TagClass (
    id UInt64,
    name String,
    url String
) ENGINE = MergeTree()
ORDER BY id;

-- Tag -> TagClass
-- CSV: TagId|TagClassId
CREATE TABLE IF NOT EXISTS ldbc.Tag_hasType_TagClass (
    TagId UInt64,
    TagClassId UInt64
) ENGINE = MergeTree()
ORDER BY (TagId, TagClassId);

-- TagClass hierarchy
-- CSV: TagClass1Id|TagClass2Id
CREATE TABLE IF NOT EXISTS ldbc.TagClass_isSubclassOf_TagClass (
    TagClass1Id UInt64,
    TagClass2Id UInt64
) ENGINE = MergeTree()
ORDER BY (TagClass1Id, TagClass2Id);

-- =============================================================================
-- DYNAMIC TABLES (social network data)
-- =============================================================================

-- Person
-- CSV: creationDate|id|firstName|lastName|gender|birthday|locationIP|browserUsed|language|email
CREATE TABLE IF NOT EXISTS ldbc.Person (
    creationDate Int64,  -- epoch milliseconds
    id UInt64,
    firstName String,
    lastName String,
    gender String,
    birthday Int64,  -- epoch milliseconds
    locationIP String,
    browserUsed String,
    speaks Array(String),  -- will be populated from 'language' field via splitByChar
    email Array(String)    -- will be populated from 'email' field via splitByChar
) ENGINE = MergeTree()
ORDER BY id;

-- Person location
-- CSV: creationDate|PersonId|CityId
CREATE TABLE IF NOT EXISTS ldbc.Person_isLocatedIn_Place (
    creationDate Int64,
    PersonId UInt64,
    CityId UInt64  -- Note: CSV calls it CityId but it's actually a Place id
) ENGINE = MergeTree()
ORDER BY (PersonId, CityId);

-- Person interests
-- CSV: creationDate|PersonId|TagId
CREATE TABLE IF NOT EXISTS ldbc.Person_hasInterest_Tag (
    creationDate Int64,
    PersonId UInt64,
    TagId UInt64
) ENGINE = MergeTree()
ORDER BY (PersonId, TagId);

-- Person work
-- CSV: creationDate|PersonId|CompanyId|workFrom
CREATE TABLE IF NOT EXISTS ldbc.Person_workAt_Organisation (
    creationDate Int64,
    PersonId UInt64,
    CompanyId UInt64,  -- Note: CSV calls it CompanyId but it's an Organisation id
    workFrom Int32
) ENGINE = MergeTree()
ORDER BY (PersonId, CompanyId);

-- Person study
-- CSV: creationDate|PersonId|UniversityId|classYear
CREATE TABLE IF NOT EXISTS ldbc.Person_studyAt_Organisation (
    creationDate Int64,
    PersonId UInt64,
    UniversityId UInt64,  -- Note: CSV calls it UniversityId but it's an Organisation id
    classYear Int32
) ENGINE = MergeTree()
ORDER BY (PersonId, UniversityId);

-- Person knows Person (bidirectional friendship)
-- CSV: creationDate|Person1Id|Person2Id
CREATE TABLE IF NOT EXISTS ldbc.Person_knows_Person (
    creationDate Int64,  -- epoch milliseconds
    Person1Id UInt64,
    Person2Id UInt64
) ENGINE = MergeTree()
ORDER BY (Person1Id, Person2Id);

-- Forum
-- CSV: creationDate|id|title
CREATE TABLE IF NOT EXISTS ldbc.Forum (
    creationDate Int64,  -- epoch milliseconds
    id UInt64,
    title String
) ENGINE = MergeTree()
ORDER BY id;

-- Forum moderator
-- CSV: creationDate|ForumId|PersonId
CREATE TABLE IF NOT EXISTS ldbc.Forum_hasModerator_Person (
    creationDate Int64,
    ForumId UInt64,
    PersonId UInt64
) ENGINE = MergeTree()
ORDER BY (ForumId, PersonId);

-- Forum members
-- CSV: creationDate|ForumId|PersonId
CREATE TABLE IF NOT EXISTS ldbc.Forum_hasMember_Person (
    creationDate Int64,
    ForumId UInt64,
    PersonId UInt64
) ENGINE = MergeTree()
ORDER BY (ForumId, PersonId);

-- Forum tags
-- CSV: creationDate|ForumId|TagId
CREATE TABLE IF NOT EXISTS ldbc.Forum_hasTag_Tag (
    creationDate Int64,
    ForumId UInt64,
    TagId UInt64
) ENGINE = MergeTree()
ORDER BY (ForumId, TagId);

-- Post
-- CSV: creationDate|id|imageFile|locationIP|browserUsed|language|content|length
CREATE TABLE IF NOT EXISTS ldbc.Post (
    creationDate Int64,  -- epoch milliseconds
    id UInt64,
    imageFile String,
    locationIP String,
    browserUsed String,
    language String,
    content String,
    length UInt32
) ENGINE = MergeTree()
ORDER BY id;

-- Post creator
-- CSV: creationDate|PostId|PersonId
CREATE TABLE IF NOT EXISTS ldbc.Post_hasCreator_Person (
    creationDate Int64,
    PostId UInt64,
    PersonId UInt64
) ENGINE = MergeTree()
ORDER BY (PostId, PersonId);

-- Post location
-- CSV: creationDate|PostId|CountryId
CREATE TABLE IF NOT EXISTS ldbc.Post_isLocatedIn_Place (
    creationDate Int64,
    PostId UInt64,
    CountryId UInt64  -- Note: CSV calls it CountryId but it's a Place id
) ENGINE = MergeTree()
ORDER BY (PostId, CountryId);

-- Post tags
-- CSV: creationDate|PostId|TagId
CREATE TABLE IF NOT EXISTS ldbc.Post_hasTag_Tag (
    creationDate Int64,
    PostId UInt64,
    TagId UInt64
) ENGINE = MergeTree()
ORDER BY (PostId, TagId);

-- Forum contains Post
-- CSV: creationDate|ForumId|PostId
CREATE TABLE IF NOT EXISTS ldbc.Forum_containerOf_Post (
    creationDate Int64,
    ForumId UInt64,
    PostId UInt64
) ENGINE = MergeTree()
ORDER BY (ForumId, PostId);

-- Comment
-- CSV: creationDate|id|locationIP|browserUsed|content|length
CREATE TABLE IF NOT EXISTS ldbc.Comment (
    creationDate Int64,  -- epoch milliseconds
    id UInt64,
    locationIP String,
    browserUsed String,
    content String,
    length UInt32
) ENGINE = MergeTree()
ORDER BY id;

-- Comment creator
-- CSV: creationDate|CommentId|PersonId
CREATE TABLE IF NOT EXISTS ldbc.Comment_hasCreator_Person (
    creationDate Int64,
    CommentId UInt64,
    PersonId UInt64
) ENGINE = MergeTree()
ORDER BY (CommentId, PersonId);

-- Comment location
-- CSV: creationDate|CommentId|CountryId
CREATE TABLE IF NOT EXISTS ldbc.Comment_isLocatedIn_Place (
    creationDate Int64,
    CommentId UInt64,
    CountryId UInt64  -- Note: CSV calls it CountryId but it's a Place id
) ENGINE = MergeTree()
ORDER BY (CommentId, CountryId);

-- Comment tags
-- CSV: creationDate|CommentId|TagId
CREATE TABLE IF NOT EXISTS ldbc.Comment_hasTag_Tag (
    creationDate Int64,
    CommentId UInt64,
    TagId UInt64
) ENGINE = MergeTree()
ORDER BY (CommentId, TagId);

-- Comment replies to Post
-- CSV: creationDate|CommentId|PostId
CREATE TABLE IF NOT EXISTS ldbc.Comment_replyOf_Post (
    creationDate Int64,
    CommentId UInt64,
    PostId UInt64
) ENGINE = MergeTree()
ORDER BY (CommentId, PostId);

-- Comment replies to Comment
-- CSV: creationDate|Comment1Id|Comment2Id
CREATE TABLE IF NOT EXISTS ldbc.Comment_replyOf_Comment (
    creationDate Int64,
    Comment1Id UInt64,
    Comment2Id UInt64
) ENGINE = MergeTree()
ORDER BY (Comment1Id, Comment2Id);

-- Person likes Post
-- CSV: creationDate|PersonId|PostId
CREATE TABLE IF NOT EXISTS ldbc.Person_likes_Post (
    creationDate Int64,  -- epoch milliseconds
    PersonId UInt64,
    PostId UInt64
) ENGINE = MergeTree()
ORDER BY (PersonId, PostId);

-- Person likes Comment
-- CSV: creationDate|PersonId|CommentId
CREATE TABLE IF NOT EXISTS ldbc.Person_likes_Comment (
    creationDate Int64,  -- epoch milliseconds
    PersonId UInt64,
    CommentId UInt64
) ENGINE = MergeTree()
ORDER BY (PersonId, CommentId);

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
SELECT 
    creationDate,
    PostId AS MessageId, 
    PersonId
FROM ldbc.Post_hasCreator_Person
UNION ALL
SELECT 
    creationDate,
    CommentId AS MessageId, 
    PersonId
FROM ldbc.Comment_hasCreator_Person;

-- Unified likes view
CREATE VIEW IF NOT EXISTS ldbc.Person_likes_Message AS
SELECT 
    creationDate,
    PersonId, 
    PostId AS MessageId
FROM ldbc.Person_likes_Post
UNION ALL
SELECT 
    creationDate,
    PersonId, 
    CommentId AS MessageId
FROM ldbc.Person_likes_Comment;

-- Unified reply-of view (Comment can reply to either Post or Comment, both are Messages)
CREATE VIEW IF NOT EXISTS ldbc.Comment_replyOf_Message AS
SELECT 
    creationDate,
    CommentId, 
    PostId AS MessageId
FROM ldbc.Comment_replyOf_Post
UNION ALL
SELECT 
    creationDate,
    Comment1Id AS CommentId, 
    Comment2Id AS MessageId
FROM ldbc.Comment_replyOf_Comment;
