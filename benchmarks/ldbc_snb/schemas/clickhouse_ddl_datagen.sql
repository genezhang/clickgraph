-- LDBC SNB Interactive v1 Schema for ClickHouse
-- Matches the output format of ldbc/datagen-standalone Docker image
-- Uses composite-projected-fk format with epoch-millis timestamps

-- Create database
CREATE DATABASE IF NOT EXISTS ldbc;

-- =============================================================================
-- STATIC TABLES (reference data that doesn't change)
-- =============================================================================

-- Place hierarchy: Continent -> Country -> City
-- Datagen columns: id|name|url|type
CREATE TABLE IF NOT EXISTS ldbc.Place (
    id Int32,
    name String,
    url String,
    type String  -- 'Continent', 'Country', 'City'
) ENGINE = MergeTree()
ORDER BY id;

-- Place hierarchy relationship
-- Datagen columns: Place1Id|Place2Id
CREATE TABLE IF NOT EXISTS ldbc.Place_isPartOf_Place (
    Place1Id Int32,
    Place2Id Int32
) ENGINE = MergeTree()
ORDER BY (Place1Id, Place2Id);

-- Organisation: University or Company
-- Datagen columns: id|type|name|url
CREATE TABLE IF NOT EXISTS ldbc.Organisation (
    id Int32,
    type String,  -- 'University', 'Company'
    name String,
    url String
) ENGINE = MergeTree()
ORDER BY id;

-- Organisation location
-- Datagen columns: OrganisationId|PlaceId
CREATE TABLE IF NOT EXISTS ldbc.Organisation_isLocatedIn_Place (
    OrganisationId Int32,
    PlaceId Int32
) ENGINE = MergeTree()
ORDER BY (OrganisationId, PlaceId);

-- Tag
-- Datagen columns: id|name|url
CREATE TABLE IF NOT EXISTS ldbc.Tag (
    id Int32,
    name String,
    url String
) ENGINE = MergeTree()
ORDER BY id;

-- TagClass
-- Datagen columns: id|name|url
CREATE TABLE IF NOT EXISTS ldbc.TagClass (
    id Int32,
    name String,
    url String
) ENGINE = MergeTree()
ORDER BY id;

-- Tag -> TagClass
-- Datagen columns: TagId|TagClassId
CREATE TABLE IF NOT EXISTS ldbc.Tag_hasType_TagClass (
    TagId Int32,
    TagClassId Int32
) ENGINE = MergeTree()
ORDER BY (TagId, TagClassId);

-- TagClass hierarchy
-- Datagen columns: TagClass1Id|TagClass2Id
CREATE TABLE IF NOT EXISTS ldbc.TagClass_isSubclassOf_TagClass (
    TagClass1Id Int32,
    TagClass2Id Int32
) ENGINE = MergeTree()
ORDER BY (TagClass1Id, TagClass2Id);

-- =============================================================================
-- DYNAMIC TABLES (social network data)
-- =============================================================================

-- Person
-- Datagen columns: creationDate|id|firstName|lastName|gender|birthday|locationIP|browserUsed|language|email
CREATE TABLE IF NOT EXISTS ldbc.Person (
    creationDate Int64,  -- epoch milliseconds
    id Int64,
    firstName String,
    lastName String,
    gender String,
    birthday Int64,  -- epoch milliseconds
    locationIP String,
    browserUsed String,
    language String,  -- semicolon-separated
    email String  -- semicolon-separated
) ENGINE = MergeTree()
ORDER BY id;

-- Person location
-- Datagen columns: creationDate|PersonId|CityId
CREATE TABLE IF NOT EXISTS ldbc.Person_isLocatedIn_Place (
    creationDate Int64,
    PersonId Int64,
    CityId Int32
) ENGINE = MergeTree()
ORDER BY (PersonId, CityId);

-- Person interests
-- Datagen columns: creationDate|PersonId|TagId
CREATE TABLE IF NOT EXISTS ldbc.Person_hasInterest_Tag (
    creationDate Int64,
    PersonId Int64,
    TagId Int32
) ENGINE = MergeTree()
ORDER BY (PersonId, TagId);

-- Person study at (university)
-- Datagen columns: creationDate|PersonId|UniversityId|classYear
CREATE TABLE IF NOT EXISTS ldbc.Person_studyAt_Organisation (
    creationDate Int64,
    PersonId Int64,
    UniversityId Int32,
    classYear Int32
) ENGINE = MergeTree()
ORDER BY (PersonId, UniversityId);

-- Person work at (company)
-- Datagen columns: creationDate|PersonId|CompanyId|workFrom
CREATE TABLE IF NOT EXISTS ldbc.Person_workAt_Organisation (
    creationDate Int64,
    PersonId Int64,
    CompanyId Int32,
    workFrom Int32
) ENGINE = MergeTree()
ORDER BY (PersonId, CompanyId);

-- Person knows Person (friendship)
-- Datagen columns: creationDate|Person1Id|Person2Id
CREATE TABLE IF NOT EXISTS ldbc.Person_knows_Person (
    creationDate Int64,
    Person1Id Int64,
    Person2Id Int64
) ENGINE = MergeTree()
ORDER BY (Person1Id, Person2Id);

-- Forum
-- Datagen columns: creationDate|id|title
CREATE TABLE IF NOT EXISTS ldbc.Forum (
    creationDate Int64,
    id Int64,
    title String
) ENGINE = MergeTree()
ORDER BY id;

-- Forum moderator
-- Datagen columns: creationDate|ForumId|PersonId
CREATE TABLE IF NOT EXISTS ldbc.Forum_hasModerator_Person (
    creationDate Int64,
    ForumId Int64,
    PersonId Int64
) ENGINE = MergeTree()
ORDER BY (ForumId, PersonId);

-- Forum membership
-- Datagen columns: creationDate|ForumId|PersonId
CREATE TABLE IF NOT EXISTS ldbc.Forum_hasMember_Person (
    creationDate Int64,
    ForumId Int64,
    PersonId Int64
) ENGINE = MergeTree()
ORDER BY (ForumId, PersonId);

-- Forum tags
-- Datagen columns: creationDate|ForumId|TagId
CREATE TABLE IF NOT EXISTS ldbc.Forum_hasTag_Tag (
    creationDate Int64,
    ForumId Int64,
    TagId Int32
) ENGINE = MergeTree()
ORDER BY (ForumId, TagId);

-- Post
-- Datagen columns: creationDate|id|imageFile|locationIP|browserUsed|language|content|length
CREATE TABLE IF NOT EXISTS ldbc.Post (
    creationDate Int64,
    id Int64,
    imageFile String,
    locationIP String,
    browserUsed String,
    language String,
    content String,
    length Int32
) ENGINE = MergeTree()
ORDER BY id;

-- Post creator
-- Datagen columns: creationDate|PostId|PersonId
CREATE TABLE IF NOT EXISTS ldbc.Post_hasCreator_Person (
    creationDate Int64,
    PostId Int64,
    PersonId Int64
) ENGINE = MergeTree()
ORDER BY (PostId, PersonId);

-- Post location
-- Datagen columns: creationDate|PostId|CountryId
CREATE TABLE IF NOT EXISTS ldbc.Post_isLocatedIn_Place (
    creationDate Int64,
    PostId Int64,
    CountryId Int32
) ENGINE = MergeTree()
ORDER BY (PostId, CountryId);

-- Post in forum (Forum contains Post)
-- Datagen columns: creationDate|ForumId|PostId
CREATE TABLE IF NOT EXISTS ldbc.Forum_containerOf_Post (
    creationDate Int64,
    ForumId Int64,
    PostId Int64
) ENGINE = MergeTree()
ORDER BY (ForumId, PostId);

-- Post tags
-- Datagen columns: creationDate|PostId|TagId
CREATE TABLE IF NOT EXISTS ldbc.Post_hasTag_Tag (
    creationDate Int64,
    PostId Int64,
    TagId Int32
) ENGINE = MergeTree()
ORDER BY (PostId, TagId);

-- Person likes Post
-- Datagen columns: creationDate|PersonId|PostId
CREATE TABLE IF NOT EXISTS ldbc.Person_likes_Post (
    creationDate Int64,
    PersonId Int64,
    PostId Int64
) ENGINE = MergeTree()
ORDER BY (PersonId, PostId);

-- Comment
-- Datagen columns: creationDate|id|locationIP|browserUsed|content|length
CREATE TABLE IF NOT EXISTS ldbc.Comment (
    creationDate Int64,
    id Int64,
    locationIP String,
    browserUsed String,
    content String,
    length Int32
) ENGINE = MergeTree()
ORDER BY id;

-- Comment creator
-- Datagen columns: creationDate|CommentId|PersonId
CREATE TABLE IF NOT EXISTS ldbc.Comment_hasCreator_Person (
    creationDate Int64,
    CommentId Int64,
    PersonId Int64
) ENGINE = MergeTree()
ORDER BY (CommentId, PersonId);

-- Comment location
-- Datagen columns: creationDate|CommentId|CountryId
CREATE TABLE IF NOT EXISTS ldbc.Comment_isLocatedIn_Place (
    creationDate Int64,
    CommentId Int64,
    CountryId Int32
) ENGINE = MergeTree()
ORDER BY (CommentId, CountryId);

-- Comment tags
-- Datagen columns: creationDate|CommentId|TagId
CREATE TABLE IF NOT EXISTS ldbc.Comment_hasTag_Tag (
    creationDate Int64,
    CommentId Int64,
    TagId Int32
) ENGINE = MergeTree()
ORDER BY (CommentId, TagId);

-- Comment replies to Post
-- Datagen columns: creationDate|CommentId|PostId
CREATE TABLE IF NOT EXISTS ldbc.Comment_replyOf_Post (
    creationDate Int64,
    CommentId Int64,
    PostId Int64
) ENGINE = MergeTree()
ORDER BY (CommentId, PostId);

-- Comment replies to Comment
-- Datagen columns: creationDate|Comment1Id|Comment2Id
CREATE TABLE IF NOT EXISTS ldbc.Comment_replyOf_Comment (
    creationDate Int64,
    Comment1Id Int64,
    Comment2Id Int64
) ENGINE = MergeTree()
ORDER BY (Comment1Id, Comment2Id);

-- Person likes Comment
-- Datagen columns: creationDate|PersonId|CommentId
CREATE TABLE IF NOT EXISTS ldbc.Person_likes_Comment (
    creationDate Int64,
    PersonId Int64,
    CommentId Int64
) ENGINE = MergeTree()
ORDER BY (PersonId, CommentId);

-- =============================================================================
-- VIEWS for graph queries (simplified access patterns)
-- =============================================================================

-- View for Person with location (City name)
CREATE VIEW IF NOT EXISTS ldbc.PersonWithLocation AS
SELECT 
    p.*,
    pl.name AS cityName,
    pl2.name AS countryName
FROM ldbc.Person p
LEFT JOIN ldbc.Person_isLocatedIn_Place pip ON p.id = pip.PersonId
LEFT JOIN ldbc.Place pl ON pip.CityId = pl.id
LEFT JOIN ldbc.Place_isPartOf_Place pp ON pl.id = pp.Place1Id
LEFT JOIN ldbc.Place pl2 ON pp.Place2Id = pl2.id AND pl2.type = 'Country';

-- View for friendships (bidirectional)
CREATE VIEW IF NOT EXISTS ldbc.Knows AS
SELECT Person1Id AS person1, Person2Id AS person2, creationDate
FROM ldbc.Person_knows_Person
UNION ALL
SELECT Person2Id AS person1, Person1Id AS person2, creationDate  
FROM ldbc.Person_knows_Person;
