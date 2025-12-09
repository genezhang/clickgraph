-- LDBC SNB Interactive v1 - ClickHouse DDL
-- Schema for ldbc/datagen-standalone output (sf0.003 and above)
-- Column names match datagen CSV headers exactly
-- Column order matches CSV file order for direct loading

CREATE DATABASE IF NOT EXISTS ldbc;
USE ldbc;

-- =============================================================================
-- STATIC ENTITIES (from static/ directory)
-- =============================================================================

-- Place: Cities, Countries, Continents
CREATE TABLE IF NOT EXISTS Place (
    id UInt64,
    name String,
    url String,
    type String
) ENGINE = MergeTree() ORDER BY id;

-- TagClass: Hierarchical classification of Tags  
CREATE TABLE IF NOT EXISTS TagClass (
    id UInt64,
    name String,
    url String
) ENGINE = MergeTree() ORDER BY id;

-- Tag: Content categorization
CREATE TABLE IF NOT EXISTS Tag (
    id UInt64,
    name String,
    url String
) ENGINE = MergeTree() ORDER BY id;

-- Organisation: Universities and Companies
CREATE TABLE IF NOT EXISTS Organisation (
    id UInt64,
    name String,
    url String,
    type String
) ENGINE = MergeTree() ORDER BY id;

-- =============================================================================
-- STATIC RELATIONSHIPS
-- =============================================================================

-- Place_isPartOf_Place: City->Country->Continent hierarchy
CREATE TABLE IF NOT EXISTS Place_isPartOf_Place (
    Place_id UInt64,
    Place_id_2 UInt64
) ENGINE = MergeTree() ORDER BY (Place_id, Place_id_2);

-- Organisation_isLocatedIn_Place
CREATE TABLE IF NOT EXISTS Organisation_isLocatedIn_Place (
    Organisation_id UInt64,
    Place_id UInt64
) ENGINE = MergeTree() ORDER BY (Organisation_id, Place_id);

-- Tag_hasType_TagClass
CREATE TABLE IF NOT EXISTS Tag_hasType_TagClass (
    Tag_id UInt64,
    TagClass_id UInt64
) ENGINE = MergeTree() ORDER BY (Tag_id, TagClass_id);

-- TagClass_isSubclassOf_TagClass
CREATE TABLE IF NOT EXISTS TagClass_isSubclassOf_TagClass (
    TagClass_id UInt64,
    TagClass_id_2 UInt64
) ENGINE = MergeTree() ORDER BY (TagClass_id, TagClass_id_2);

-- =============================================================================
-- DYNAMIC ENTITIES (from dynamic/ directory)
-- Note: Column order matches CSV output (creationDate first for timestamped entities)
-- =============================================================================

-- Person: Central node type
CREATE TABLE IF NOT EXISTS Person (
    creationDate Int64,
    id UInt64,
    firstName String,
    lastName String,
    gender String,
    birthday Int64,
    locationIP String,
    browserUsed String,
    language String,   -- semicolon-separated in CSV
    email String       -- semicolon-separated in CSV
) ENGINE = MergeTree() ORDER BY id;

-- Forum: Container for Posts
CREATE TABLE IF NOT EXISTS Forum (
    creationDate Int64,
    id UInt64,
    title String
) ENGINE = MergeTree() ORDER BY id;

-- Post: Content in Forums
CREATE TABLE IF NOT EXISTS Post (
    creationDate Int64,
    id UInt64,
    imageFile String,
    locationIP String,
    browserUsed String,
    language String,
    content String,
    length UInt32
) ENGINE = MergeTree() ORDER BY id;

-- Comment: Reply to Post or Comment
CREATE TABLE IF NOT EXISTS Comment (
    id UInt64,
    creationDate Int64,
    locationIP String,
    browserUsed String,
    content String,
    length UInt32
) ENGINE = MergeTree() ORDER BY id;

-- =============================================================================
-- DYNAMIC RELATIONSHIPS (from dynamic/ directory)
-- All have creationDate as first column
-- =============================================================================

-- Person KNOWS Person
CREATE TABLE IF NOT EXISTS Person_knows_Person (
    creationDate Int64,
    Person1Id UInt64,
    Person2Id UInt64
) ENGINE = MergeTree() ORDER BY (Person1Id, Person2Id);

-- Person IS_LOCATED_IN City
CREATE TABLE IF NOT EXISTS Person_isLocatedIn_City (
    creationDate Int64,
    PersonId UInt64,
    CityId UInt64
) ENGINE = MergeTree() ORDER BY (PersonId, CityId);

-- Person HAS_INTEREST Tag
CREATE TABLE IF NOT EXISTS Person_hasInterest_Tag (
    creationDate Int64,
    PersonId UInt64,
    TagId UInt64
) ENGINE = MergeTree() ORDER BY (PersonId, TagId);

-- Person STUDY_AT University
CREATE TABLE IF NOT EXISTS Person_studyAt_University (
    creationDate Int64,
    PersonId UInt64,
    UniversityId UInt64,
    classYear UInt16
) ENGINE = MergeTree() ORDER BY (PersonId, UniversityId);

-- Person WORK_AT Company
CREATE TABLE IF NOT EXISTS Person_workAt_Company (
    creationDate Int64,
    PersonId UInt64,
    CompanyId UInt64,
    workFrom UInt16
) ENGINE = MergeTree() ORDER BY (PersonId, CompanyId);

-- Person LIKES Post
CREATE TABLE IF NOT EXISTS Person_likes_Post (
    creationDate Int64,
    PersonId UInt64,
    PostId UInt64
) ENGINE = MergeTree() ORDER BY (PersonId, PostId);

-- Person LIKES Comment
CREATE TABLE IF NOT EXISTS Person_likes_Comment (
    creationDate Int64,
    PersonId UInt64,
    CommentId UInt64
) ENGINE = MergeTree() ORDER BY (PersonId, CommentId);

-- Forum HAS_MODERATOR Person
CREATE TABLE IF NOT EXISTS Forum_hasModerator_Person (
    creationDate Int64,
    ForumId UInt64,
    PersonId UInt64
) ENGINE = MergeTree() ORDER BY (ForumId, PersonId);

-- Forum HAS_MEMBER Person
CREATE TABLE IF NOT EXISTS Forum_hasMember_Person (
    creationDate Int64,
    ForumId UInt64,
    PersonId UInt64
) ENGINE = MergeTree() ORDER BY (ForumId, PersonId);

-- Forum HAS_TAG Tag
CREATE TABLE IF NOT EXISTS Forum_hasTag_Tag (
    creationDate Int64,
    ForumId UInt64,
    TagId UInt64
) ENGINE = MergeTree() ORDER BY (ForumId, TagId);

-- Forum CONTAINER_OF Post
CREATE TABLE IF NOT EXISTS Forum_containerOf_Post (
    creationDate Int64,
    ForumId UInt64,
    PostId UInt64
) ENGINE = MergeTree() ORDER BY (ForumId, PostId);

-- Post HAS_CREATOR Person
CREATE TABLE IF NOT EXISTS Post_hasCreator_Person (
    creationDate Int64,
    PostId UInt64,
    PersonId UInt64
) ENGINE = MergeTree() ORDER BY (PostId, PersonId);

-- Post IS_LOCATED_IN Country
CREATE TABLE IF NOT EXISTS Post_isLocatedIn_Country (
    creationDate Int64,
    PostId UInt64,
    CountryId UInt64
) ENGINE = MergeTree() ORDER BY (PostId, CountryId);

-- Post HAS_TAG Tag
CREATE TABLE IF NOT EXISTS Post_hasTag_Tag (
    creationDate Int64,
    PostId UInt64,
    TagId UInt64
) ENGINE = MergeTree() ORDER BY (PostId, TagId);

-- Comment HAS_CREATOR Person
CREATE TABLE IF NOT EXISTS Comment_hasCreator_Person (
    creationDate Int64,
    CommentId UInt64,
    PersonId UInt64
) ENGINE = MergeTree() ORDER BY (CommentId, PersonId);

-- Comment IS_LOCATED_IN Country
CREATE TABLE IF NOT EXISTS Comment_isLocatedIn_Country (
    creationDate Int64,
    CommentId UInt64,
    CountryId UInt64
) ENGINE = MergeTree() ORDER BY (CommentId, CountryId);

-- Comment HAS_TAG Tag
CREATE TABLE IF NOT EXISTS Comment_hasTag_Tag (
    creationDate Int64,
    CommentId UInt64,
    TagId UInt64
) ENGINE = MergeTree() ORDER BY (CommentId, TagId);

-- Comment REPLY_OF Post
CREATE TABLE IF NOT EXISTS Comment_replyOf_Post (
    creationDate Int64,
    CommentId UInt64,
    PostId UInt64
) ENGINE = MergeTree() ORDER BY (CommentId, PostId);

-- Comment REPLY_OF Comment
CREATE TABLE IF NOT EXISTS Comment_replyOf_Comment (
    creationDate Int64,
    Comment1Id UInt64,
    Comment2Id UInt64
) ENGINE = MergeTree() ORDER BY (Comment1Id, Comment2Id);
