-- sf1 DDL + Data Loading Script
--
-- Creates the ldbc database, all tables, loads sf1 CsvBasic v1 data,
-- creates union views (Message, etc.), and adds ALIAS columns for
-- column name normalization.
--
-- Prerequisite: sf1 data extracted at /data/sf1/ inside ClickHouse container
-- (mount benchmarks/ldbc_snb/data/ as /data/)
--
-- Usage (one statement at a time via clickhouse-client):
--   cat sf1_ddl_load.sql | clickhouse-client --multiquery
-- Or via HTTP (each statement separately):
--   while IFS= read -r stmt; do
--     curl 'http://localhost:18123/?user=test_user&password=test_pass' --data-binary "$stmt"
--   done < <(grep -v '^--' sf1_ddl_load.sql | sed '/^$/d')

-- ============================================================
-- 1. DATABASE
-- ============================================================
CREATE DATABASE IF NOT EXISTS ldbc;

-- ============================================================
-- 2. NODE TABLES
-- ============================================================

-- Person (without speaks/email — loaded separately and aggregated)
CREATE TABLE IF NOT EXISTS ldbc.Person
(
    `creationDate` Int64,
    `id` UInt64,
    `firstName` String,
    `lastName` String,
    `gender` String,
    `birthday` Int64,
    `locationIP` String,
    `browserUsed` String,
    `speaks` Array(String),
    `email` Array(String)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS ldbc.Comment
(
    `creationDate` Int64,
    `id` UInt64,
    `locationIP` String,
    `browserUsed` String,
    `content` String,
    `length` UInt32
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS ldbc.Post
(
    `creationDate` Int64,
    `id` UInt64,
    `imageFile` String,
    `locationIP` String,
    `browserUsed` String,
    `language` String,
    `content` String,
    `length` UInt32
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS ldbc.Forum
(
    `creationDate` Int64,
    `id` UInt64,
    `title` String
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS ldbc.Organisation
(
    `id` UInt64,
    `type` String,
    `name` String,
    `url` String
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS ldbc.Place
(
    `id` UInt64,
    `name` String,
    `url` String,
    `type` String
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS ldbc.Tag
(
    `id` UInt64,
    `name` String,
    `url` String
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS ldbc.TagClass
(
    `id` UInt64,
    `name` String,
    `url` String
) ENGINE = MergeTree() ORDER BY id;

-- ============================================================
-- 3. EDGE TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS ldbc.Person_knows_Person
(
    `creationDate` Int64,
    `Person1Id` UInt64,
    `Person2Id` UInt64
) ENGINE = MergeTree() ORDER BY (Person1Id, Person2Id);

CREATE TABLE IF NOT EXISTS ldbc.Person_studyAt_Organisation
(
    `creationDate` Int64,
    `PersonId` UInt64,
    `UniversityId` UInt64,
    `classYear` Int32
) ENGINE = MergeTree() ORDER BY (PersonId, UniversityId);

CREATE TABLE IF NOT EXISTS ldbc.Person_workAt_Organisation
(
    `creationDate` Int64,
    `PersonId` UInt64,
    `CompanyId` UInt64,
    `workFrom` Int32
) ENGINE = MergeTree() ORDER BY (PersonId, CompanyId);

CREATE TABLE IF NOT EXISTS ldbc.Person_hasInterest_Tag
(
    `creationDate` Int64,
    `PersonId` UInt64,
    `TagId` UInt64
) ENGINE = MergeTree() ORDER BY (PersonId, TagId);

CREATE TABLE IF NOT EXISTS ldbc.Person_isLocatedIn_Place
(
    `creationDate` Int64,
    `PersonId` UInt64,
    `CityId` UInt64
) ENGINE = MergeTree() ORDER BY (PersonId, CityId);

CREATE TABLE IF NOT EXISTS ldbc.Person_likes_Comment
(
    `creationDate` Int64,
    `PersonId` UInt64,
    `CommentId` UInt64
) ENGINE = MergeTree() ORDER BY (PersonId, CommentId);

CREATE TABLE IF NOT EXISTS ldbc.Person_likes_Post
(
    `creationDate` Int64,
    `PersonId` UInt64,
    `PostId` UInt64
) ENGINE = MergeTree() ORDER BY (PersonId, PostId);

CREATE TABLE IF NOT EXISTS ldbc.Comment_hasCreator_Person
(
    `creationDate` Int64,
    `CommentId` UInt64,
    `PersonId` UInt64
) ENGINE = MergeTree() ORDER BY (CommentId, PersonId);

CREATE TABLE IF NOT EXISTS ldbc.Comment_hasTag_Tag
(
    `creationDate` Int64,
    `CommentId` UInt64,
    `TagId` UInt64
) ENGINE = MergeTree() ORDER BY (CommentId, TagId);

CREATE TABLE IF NOT EXISTS ldbc.Comment_isLocatedIn_Place
(
    `creationDate` Int64,
    `CommentId` UInt64,
    `CountryId` UInt64
) ENGINE = MergeTree() ORDER BY (CommentId, CountryId);

CREATE TABLE IF NOT EXISTS ldbc.Comment_replyOf_Comment
(
    `creationDate` Int64,
    `Comment1Id` UInt64,
    `Comment2Id` UInt64
) ENGINE = MergeTree() ORDER BY (Comment1Id, Comment2Id);

CREATE TABLE IF NOT EXISTS ldbc.Comment_replyOf_Post
(
    `creationDate` Int64,
    `CommentId` UInt64,
    `PostId` UInt64
) ENGINE = MergeTree() ORDER BY (CommentId, PostId);

CREATE TABLE IF NOT EXISTS ldbc.Post_hasCreator_Person
(
    `creationDate` Int64,
    `PostId` UInt64,
    `PersonId` UInt64
) ENGINE = MergeTree() ORDER BY (PostId, PersonId);

CREATE TABLE IF NOT EXISTS ldbc.Post_hasTag_Tag
(
    `creationDate` Int64,
    `PostId` UInt64,
    `TagId` UInt64
) ENGINE = MergeTree() ORDER BY (PostId, TagId);

CREATE TABLE IF NOT EXISTS ldbc.Post_isLocatedIn_Place
(
    `creationDate` Int64,
    `PostId` UInt64,
    `CountryId` UInt64
) ENGINE = MergeTree() ORDER BY (PostId, CountryId);

CREATE TABLE IF NOT EXISTS ldbc.Forum_containerOf_Post
(
    `creationDate` Int64,
    `ForumId` UInt64,
    `PostId` UInt64
) ENGINE = MergeTree() ORDER BY (ForumId, PostId);

CREATE TABLE IF NOT EXISTS ldbc.Forum_hasMember_Person
(
    `creationDate` Int64,
    `ForumId` UInt64,
    `PersonId` UInt64
) ENGINE = MergeTree() ORDER BY (ForumId, PersonId);

CREATE TABLE IF NOT EXISTS ldbc.Forum_hasModerator_Person
(
    `creationDate` Int64,
    `ForumId` UInt64,
    `PersonId` UInt64
) ENGINE = MergeTree() ORDER BY (ForumId, PersonId);

CREATE TABLE IF NOT EXISTS ldbc.Forum_hasTag_Tag
(
    `creationDate` Int64,
    `ForumId` UInt64,
    `TagId` UInt64
) ENGINE = MergeTree() ORDER BY (ForumId, TagId);

CREATE TABLE IF NOT EXISTS ldbc.Organisation_isLocatedIn_Place
(
    `OrganisationId` UInt64,
    `PlaceId` UInt64
) ENGINE = MergeTree() ORDER BY (OrganisationId, PlaceId);

CREATE TABLE IF NOT EXISTS ldbc.Place_isPartOf_Place
(
    `Place1Id` UInt64,
    `Place2Id` UInt64
) ENGINE = MergeTree() ORDER BY (Place1Id, Place2Id);

CREATE TABLE IF NOT EXISTS ldbc.Tag_hasType_TagClass
(
    `TagId` UInt64,
    `TagClassId` UInt64
) ENGINE = MergeTree() ORDER BY (TagId, TagClassId);

CREATE TABLE IF NOT EXISTS ldbc.TagClass_isSubclassOf_TagClass
(
    `TagClass1Id` UInt64,
    `TagClass2Id` UInt64
) ENGINE = MergeTree() ORDER BY (TagClass1Id, TagClass2Id);
