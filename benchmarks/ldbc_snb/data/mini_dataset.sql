-- LDBC SNB Mini Dataset for Functional Testing
-- 5-10 rows per table, covers all relationship patterns
-- Database: ldbc_mini

CREATE DATABASE IF NOT EXISTS ldbc_mini;

-- Place hierarchy: Continent -> Country -> City
CREATE TABLE IF NOT EXISTS ldbc_mini.Place (
    id UInt64, name String, url String, type String
) ENGINE = Memory;

INSERT INTO ldbc_mini.Place VALUES
(1, 'Earth', 'http://dbpedia.org/resource/Earth', 'Continent'),
(2, 'Europe', 'http://dbpedia.org/resource/Europe', 'Continent'),
(3, 'United_States', 'http://dbpedia.org/resource/United_States', 'Country'),
(4, 'Germany', 'http://dbpedia.org/resource/Germany', 'Country'),
(5, 'Angola', 'http://dbpedia.org/resource/Angola', 'Country'),
(6, 'Colombia', 'http://dbpedia.org/resource/Colombia', 'Country'),
(7, 'New_York', 'http://dbpedia.org/resource/New_York', 'City'),
(8, 'Berlin', 'http://dbpedia.org/resource/Berlin', 'City'),
(9, 'Munich', 'http://dbpedia.org/resource/Munich', 'City'),
(10, 'Bogota', 'http://dbpedia.org/resource/Bogota', 'City');

CREATE TABLE IF NOT EXISTS ldbc_mini.Place_isPartOf_Place (
    Place1Id UInt64, Place2Id UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Place_isPartOf_Place VALUES
(7, 3), (8, 4), (9, 4), (10, 6), (3, 2), (4, 2), (5, 1), (6, 2);

CREATE TABLE IF NOT EXISTS ldbc_mini.Organisation (
    id UInt64, type String, name String, url String
) ENGINE = Memory;

INSERT INTO ldbc_mini.Organisation VALUES
(1, 'University', 'MIT', 'http://dbpedia.org/resource/MIT'),
(2, 'University', 'TU_Berlin', 'http://dbpedia.org/resource/TU_Berlin'),
(3, 'Company', 'Google', 'http://dbpedia.org/resource/Google'),
(4, 'Company', 'SAP', 'http://dbpedia.org/resource/SAP');

CREATE TABLE IF NOT EXISTS ldbc_mini.Organisation_isLocatedIn_Place (
    OrganisationId UInt64, PlaceId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Organisation_isLocatedIn_Place VALUES
(1, 3), (2, 4), (3, 3), (4, 4);

CREATE TABLE IF NOT EXISTS ldbc_mini.Tag (
    id UInt64, name String, url String
) ENGINE = Memory;

INSERT INTO ldbc_mini.Tag VALUES
(1, 'Databases', 'http://dbpedia.org/resource/Database'),
(2, 'Graphs', 'http://dbpedia.org/resource/Graph'),
(3, 'ClickHouse', 'http://dbpedia.org/resource/ClickHouse'),
(4, 'Rust', 'http://dbpedia.org/resource/Rust'),
(5, 'Music', 'http://dbpedia.org/resource/Music');

CREATE TABLE IF NOT EXISTS ldbc_mini.TagClass (
    id UInt64, name String, url String
) ENGINE = Memory;

INSERT INTO ldbc_mini.TagClass VALUES
(1, 'Technology', 'http://dbpedia.org/resource/Technology'),
(2, 'Science', 'http://dbpedia.org/resource/Science'),
(3, 'Entertainment', 'http://dbpedia.org/resource/Entertainment');

CREATE TABLE IF NOT EXISTS ldbc_mini.Tag_hasType_TagClass (
    TagId UInt64, TagClassId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Tag_hasType_TagClass VALUES
(1, 1), (2, 1), (3, 1), (4, 1), (5, 3);

CREATE TABLE IF NOT EXISTS ldbc_mini.TagClass_isSubclassOf_TagClass (
    TagClass1Id UInt64, TagClass2Id UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.TagClass_isSubclassOf_TagClass VALUES
(1, 2);

CREATE TABLE IF NOT EXISTS ldbc_mini.Person (
    creationDate Int64, id UInt64, firstName String, lastName String,
    gender String, birthday Int64, locationIP String, browserUsed String,
    speaks Array(String), email Array(String)
) ENGINE = Memory;

INSERT INTO ldbc_mini.Person VALUES
(1262304000000, 1, 'Alice', 'Smith', 'female', 631152000000, '1.1.1.1', 'Chrome', ['en'], ['alice@example.com']),
(1262390400000, 2, 'Bob', 'Jones', 'male', 662688000000, '2.2.2.2', 'Firefox', ['en','de'], ['bob@example.com']),
(1262476800000, 3, 'Carol', 'Williams', 'female', 694224000000, '3.3.3.3', 'Safari', ['de'], ['carol@example.com']),
(1262563200000, 4, 'Dave', 'Brown', 'male', 725846400000, '4.4.4.4', 'Chrome', ['en'], ['dave@example.com']),
(1262649600000, 5, 'Eve', 'Davis', 'female', 757382400000, '5.5.5.5', 'Firefox', ['en','es'], ['eve@example.com']);

CREATE TABLE IF NOT EXISTS ldbc_mini.Person_isLocatedIn_Place (
    creationDate Int64, PersonId UInt64, CityId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Person_isLocatedIn_Place VALUES
(1262304000000, 1, 7), (1262390400000, 2, 8), (1262476800000, 3, 9),
(1262563200000, 4, 7), (1262649600000, 5, 10);

CREATE TABLE IF NOT EXISTS ldbc_mini.Person_hasInterest_Tag (
    creationDate Int64, PersonId UInt64, TagId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Person_hasInterest_Tag VALUES
(1262304000000, 1, 1), (1262304000000, 1, 2),
(1262390400000, 2, 1), (1262390400000, 2, 3),
(1262476800000, 3, 4), (1262476800000, 3, 5),
(1262563200000, 4, 2), (1262563200000, 4, 4),
(1262649600000, 5, 5);

CREATE TABLE IF NOT EXISTS ldbc_mini.Person_workAt_Organisation (
    creationDate Int64, PersonId UInt64, OrganisationId UInt64, workFrom Int32
) ENGINE = Memory;

INSERT INTO ldbc_mini.Person_workAt_Organisation VALUES
(1262304000000, 1, 3, 2015), (1262390400000, 2, 4, 2018), (1262649600000, 5, 3, 2020);

CREATE TABLE IF NOT EXISTS ldbc_mini.Person_studyAt_Organisation (
    creationDate Int64, PersonId UInt64, OrganisationId UInt64, classYear Int32
) ENGINE = Memory;

INSERT INTO ldbc_mini.Person_studyAt_Organisation VALUES
(1262304000000, 1, 1, 2010), (1262390400000, 2, 2, 2012), (1262476800000, 3, 2, 2014);

CREATE TABLE IF NOT EXISTS ldbc_mini.Person_knows_Person (
    creationDate Int64, Person1Id UInt64, Person2Id UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Person_knows_Person VALUES
(1262304000000, 1, 2), (1262304000000, 2, 1),
(1262390400000, 1, 3), (1262390400000, 3, 1),
(1262476800000, 2, 3), (1262476800000, 3, 2),
(1262563200000, 3, 4), (1262563200000, 4, 3),
(1262649600000, 4, 5), (1262649600000, 5, 4);

CREATE TABLE IF NOT EXISTS ldbc_mini.Forum (
    creationDate Int64, id UInt64, title String
) ENGINE = Memory;

INSERT INTO ldbc_mini.Forum VALUES
(1275350400000, 1, 'Wall of Alice'),
(1275436800000, 2, 'Wall of Bob'),
(1275523200000, 3, 'Tech Discussion');

CREATE TABLE IF NOT EXISTS ldbc_mini.Forum_hasModerator_Person (
    creationDate Int64, ForumId UInt64, PersonId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Forum_hasModerator_Person VALUES
(1275350400000, 1, 1), (1275436800000, 2, 2), (1275523200000, 3, 3);

CREATE TABLE IF NOT EXISTS ldbc_mini.Forum_hasMember_Person (
    creationDate Int64, ForumId UInt64, PersonId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Forum_hasMember_Person VALUES
(1275350400000, 1, 1), (1275350400000, 1, 2), (1275350400000, 1, 3),
(1275436800000, 2, 2), (1275436800000, 2, 4),
(1275523200000, 3, 1), (1275523200000, 3, 2), (1275523200000, 3, 3),
(1275523200000, 3, 4), (1275523200000, 3, 5);

CREATE TABLE IF NOT EXISTS ldbc_mini.Forum_hasTag_Tag (
    creationDate Int64, ForumId UInt64, TagId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Forum_hasTag_Tag VALUES
(1275523200000, 3, 1), (1275523200000, 3, 2), (1275523200000, 3, 3);

CREATE TABLE IF NOT EXISTS ldbc_mini.Post (
    creationDate Int64, id UInt64, imageFile String, locationIP String,
    browserUsed String, language String, content String, length UInt32
) ENGINE = Memory;

INSERT INTO ldbc_mini.Post VALUES
(1275350400000, 101, '', '1.1.1.1', 'Chrome', 'en', 'Hello from Alice', 16),
(1275436800000, 102, '', '2.2.2.2', 'Firefox', 'en', 'Bob writes about databases', 26),
(1276041600000, 103, '', '1.1.1.1', 'Chrome', 'en', 'Alice on graphs', 15),
(1276128000000, 104, 'photo.jpg', '3.3.3.3', 'Safari', 'de', '', 0),
(1270000000000, 105, '', '2.2.2.2', 'Firefox', 'en', 'Old post by Bob', 15),
(1278000000000, 106, '', '4.4.4.4', 'Chrome', 'en', 'Dave on Rust', 12);

CREATE TABLE IF NOT EXISTS ldbc_mini.Post_hasCreator_Person (
    creationDate Int64, PostId UInt64, PersonId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Post_hasCreator_Person VALUES
(1275350400000, 101, 1), (1275436800000, 102, 2), (1276041600000, 103, 1),
(1276128000000, 104, 3), (1270000000000, 105, 2), (1278000000000, 106, 4);

CREATE TABLE IF NOT EXISTS ldbc_mini.Post_isLocatedIn_Place (
    creationDate Int64, PostId UInt64, PlaceId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Post_isLocatedIn_Place VALUES
(1275350400000, 101, 3), (1275436800000, 102, 4), (1276041600000, 103, 3),
(1276128000000, 104, 4), (1270000000000, 105, 4), (1278000000000, 106, 3);

CREATE TABLE IF NOT EXISTS ldbc_mini.Post_hasTag_Tag (
    creationDate Int64, PostId UInt64, TagId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Post_hasTag_Tag VALUES
(1275350400000, 101, 1), (1275436800000, 102, 1), (1275436800000, 102, 3),
(1276041600000, 103, 2), (1276128000000, 104, 5), (1270000000000, 105, 1),
(1278000000000, 106, 4);

CREATE TABLE IF NOT EXISTS ldbc_mini.Forum_containerOf_Post (
    creationDate Int64, ForumId UInt64, PostId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Forum_containerOf_Post VALUES
(1275350400000, 1, 101), (1276041600000, 1, 103),
(1275436800000, 2, 102), (1270000000000, 2, 105),
(1276128000000, 3, 104), (1278000000000, 3, 106);

CREATE TABLE IF NOT EXISTS ldbc_mini.Comment (
    creationDate Int64, id UInt64, locationIP String, browserUsed String,
    content String, length UInt32
) ENGINE = Memory;

INSERT INTO ldbc_mini.Comment VALUES
(1275523200000, 201, '2.2.2.2', 'Firefox', 'Great post Alice!', 17),
(1275609600000, 202, '3.3.3.3', 'Safari', 'I agree with Bob', 16),
(1275696000000, 203, '4.4.4.4', 'Chrome', 'Interesting discussion', 21),
(1276214400000, 204, '1.1.1.1', 'Chrome', 'Nice photo Carol', 16),
(1276300800000, 205, '5.5.5.5', 'Firefox', 'Love this topic', 15);

CREATE TABLE IF NOT EXISTS ldbc_mini.Comment_hasCreator_Person (
    creationDate Int64, CommentId UInt64, PersonId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Comment_hasCreator_Person VALUES
(1275523200000, 201, 2), (1275609600000, 202, 3), (1275696000000, 203, 4),
(1276214400000, 204, 1), (1276300800000, 205, 5);

CREATE TABLE IF NOT EXISTS ldbc_mini.Comment_isLocatedIn_Place (
    creationDate Int64, CommentId UInt64, PlaceId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Comment_isLocatedIn_Place VALUES
(1275523200000, 201, 4), (1275609600000, 202, 4), (1275696000000, 203, 3),
(1276214400000, 204, 3), (1276300800000, 205, 6);

CREATE TABLE IF NOT EXISTS ldbc_mini.Comment_hasTag_Tag (
    creationDate Int64, CommentId UInt64, TagId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Comment_hasTag_Tag VALUES
(1275523200000, 201, 1), (1275609600000, 202, 1), (1275696000000, 203, 2),
(1276214400000, 204, 5), (1276300800000, 205, 4);

CREATE TABLE IF NOT EXISTS ldbc_mini.Comment_replyOf_Post (
    creationDate Int64, CommentId UInt64, PostId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Comment_replyOf_Post VALUES
(1275523200000, 201, 101), (1275609600000, 202, 102), (1275696000000, 203, 101),
(1276214400000, 204, 104), (1276300800000, 205, 106);

CREATE TABLE IF NOT EXISTS ldbc_mini.Comment_replyOf_Comment (
    creationDate Int64, Comment1Id UInt64, Comment2Id UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Comment_replyOf_Comment VALUES
(1275696000000, 203, 201);

CREATE TABLE IF NOT EXISTS ldbc_mini.Person_likes_Post (
    creationDate Int64, PersonId UInt64, PostId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Person_likes_Post VALUES
(1275400000000, 2, 101), (1275500000000, 3, 101), (1275600000000, 1, 102),
(1276200000000, 4, 103), (1276300000000, 5, 106);

CREATE TABLE IF NOT EXISTS ldbc_mini.Person_likes_Comment (
    creationDate Int64, PersonId UInt64, CommentId UInt64
) ENGINE = Memory;

INSERT INTO ldbc_mini.Person_likes_Comment VALUES
(1275600000000, 1, 201), (1275700000000, 2, 202), (1276400000000, 3, 204);

-- Views for unified Message type
CREATE VIEW IF NOT EXISTS ldbc_mini.Message AS
SELECT creationDate, id, locationIP, browserUsed, content, length, imageFile, language, 'Post' AS type
FROM ldbc_mini.Post
UNION ALL
SELECT creationDate, id, locationIP, browserUsed, content, length, '' AS imageFile, '' AS language, 'Comment' AS type
FROM ldbc_mini.Comment;

CREATE VIEW IF NOT EXISTS ldbc_mini.Message_hasCreator_Person AS
SELECT creationDate, PostId AS MessageId, PersonId FROM ldbc_mini.Post_hasCreator_Person
UNION ALL
SELECT creationDate, CommentId AS MessageId, PersonId FROM ldbc_mini.Comment_hasCreator_Person;

CREATE VIEW IF NOT EXISTS ldbc_mini.Person_likes_Message AS
SELECT creationDate, PersonId, PostId AS MessageId FROM ldbc_mini.Person_likes_Post
UNION ALL
SELECT creationDate, PersonId, CommentId AS MessageId FROM ldbc_mini.Person_likes_Comment;

CREATE VIEW IF NOT EXISTS ldbc_mini.Comment_replyOf_Message AS
SELECT creationDate, CommentId, PostId AS MessageId FROM ldbc_mini.Comment_replyOf_Post
UNION ALL
SELECT creationDate, Comment1Id AS CommentId, Comment2Id AS MessageId FROM ldbc_mini.Comment_replyOf_Comment;

CREATE OR REPLACE VIEW ldbc_mini.Message_replyOf_Message AS
SELECT CommentId AS MessageId, PostId AS TargetMessageId, creationDate FROM ldbc_mini.Comment_replyOf_Post
UNION ALL
SELECT Comment1Id AS MessageId, Comment2Id AS TargetMessageId, creationDate FROM ldbc_mini.Comment_replyOf_Comment;

-- Additional Message views
CREATE VIEW IF NOT EXISTS ldbc_mini.Message_hasTag_Tag AS
SELECT creationDate, PostId AS MessageId, TagId FROM ldbc_mini.Post_hasTag_Tag
UNION ALL
SELECT creationDate, CommentId AS MessageId, TagId FROM ldbc_mini.Comment_hasTag_Tag;

CREATE VIEW IF NOT EXISTS ldbc_mini.Message_isLocatedIn_Place AS
SELECT creationDate, PostId AS MessageId, PlaceId FROM ldbc_mini.Post_isLocatedIn_Place
UNION ALL
SELECT creationDate, CommentId AS MessageId, PlaceId FROM ldbc_mini.Comment_isLocatedIn_Place;
