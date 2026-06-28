-- LDBC SNB Mini Dataset — Delta translation of benchmarks/ldbc_snb/data/mini_dataset.sql
-- Schema renamed ldbc_mini → ldbc to match benchmarks/ldbc_snb/schemas/ldbc_snb_complete.yaml
-- Types preserved from ClickHouse layout (epoch-millis BIGINT for dates) so the
-- canonical schema's column mappings apply unchanged.

CREATE DATABASE IF NOT EXISTS ldbc;

-- Place hierarchy: Continent -> Country -> City
CREATE OR REPLACE TABLE ldbc.Place (
    id BIGINT, name STRING, url STRING, type STRING
) USING DELTA;

INSERT INTO ldbc.Place VALUES
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

CREATE OR REPLACE TABLE ldbc.Place_isPartOf_Place (
    Place1Id BIGINT, Place2Id BIGINT
) USING DELTA;

INSERT INTO ldbc.Place_isPartOf_Place VALUES
(7, 3), (8, 4), (9, 4), (10, 6), (3, 2), (4, 2), (5, 1), (6, 2);

CREATE OR REPLACE TABLE ldbc.Organisation (
    id BIGINT, type STRING, name STRING, url STRING
) USING DELTA;

INSERT INTO ldbc.Organisation VALUES
(1, 'University', 'MIT', 'http://dbpedia.org/resource/MIT'),
(2, 'University', 'TU_Berlin', 'http://dbpedia.org/resource/TU_Berlin'),
(3, 'Company', 'Google', 'http://dbpedia.org/resource/Google'),
(4, 'Company', 'SAP', 'http://dbpedia.org/resource/SAP');

CREATE OR REPLACE TABLE ldbc.Organisation_isLocatedIn_Place (
    OrganisationId BIGINT, PlaceId BIGINT
) USING DELTA;

INSERT INTO ldbc.Organisation_isLocatedIn_Place VALUES
(1, 3), (2, 4), (3, 3), (4, 4);

CREATE OR REPLACE TABLE ldbc.Tag (
    id BIGINT, name STRING, url STRING
) USING DELTA;

INSERT INTO ldbc.Tag VALUES
(1, 'Databases', 'http://dbpedia.org/resource/Database'),
(2, 'Graphs', 'http://dbpedia.org/resource/Graph'),
(3, 'ClickHouse', 'http://dbpedia.org/resource/ClickHouse'),
(4, 'Rust', 'http://dbpedia.org/resource/Rust'),
(5, 'Music', 'http://dbpedia.org/resource/Music');

CREATE OR REPLACE TABLE ldbc.TagClass (
    id BIGINT, name STRING, url STRING
) USING DELTA;

INSERT INTO ldbc.TagClass VALUES
(1, 'Technology', 'http://dbpedia.org/resource/Technology'),
(2, 'Science', 'http://dbpedia.org/resource/Science'),
(3, 'Entertainment', 'http://dbpedia.org/resource/Entertainment');

CREATE OR REPLACE TABLE ldbc.Tag_hasType_TagClass (
    TagId BIGINT, TagClassId BIGINT
) USING DELTA;

INSERT INTO ldbc.Tag_hasType_TagClass VALUES
(1, 1), (2, 1), (3, 1), (4, 1), (5, 3);

CREATE OR REPLACE TABLE ldbc.TagClass_isSubclassOf_TagClass (
    TagClass1Id BIGINT, TagClass2Id BIGINT
) USING DELTA;

INSERT INTO ldbc.TagClass_isSubclassOf_TagClass VALUES
(1, 2);

CREATE OR REPLACE TABLE ldbc.Person (
    creationDate BIGINT, id BIGINT, firstName STRING, lastName STRING,
    gender STRING, birthday BIGINT, locationIP STRING, browserUsed STRING,
    speaks ARRAY<STRING>, email ARRAY<STRING>
) USING DELTA;

INSERT INTO ldbc.Person VALUES
(1262304000000, 1, 'Alice', 'Smith', 'female', 631152000000, '1.1.1.1', 'Chrome', ARRAY('en'), ARRAY('alice@example.com')),
(1262390400000, 2, 'Bob', 'Jones', 'male', 662688000000, '2.2.2.2', 'Firefox', ARRAY('en','de'), ARRAY('bob@example.com')),
(1262476800000, 3, 'Carol', 'Williams', 'female', 694224000000, '3.3.3.3', 'Safari', ARRAY('de'), ARRAY('carol@example.com')),
(1262563200000, 4, 'Dave', 'Brown', 'male', 725846400000, '4.4.4.4', 'Chrome', ARRAY('en'), ARRAY('dave@example.com')),
(1262649600000, 5, 'Eve', 'Davis', 'female', 757382400000, '5.5.5.5', 'Firefox', ARRAY('en','es'), ARRAY('eve@example.com'));

CREATE OR REPLACE TABLE ldbc.Person_isLocatedIn_Place (
    creationDate BIGINT, PersonId BIGINT, CityId BIGINT
) USING DELTA;

INSERT INTO ldbc.Person_isLocatedIn_Place VALUES
(1262304000000, 1, 7), (1262390400000, 2, 8), (1262476800000, 3, 9),
(1262563200000, 4, 7), (1262649600000, 5, 10);

CREATE OR REPLACE TABLE ldbc.Person_hasInterest_Tag (
    creationDate BIGINT, PersonId BIGINT, TagId BIGINT
) USING DELTA;

INSERT INTO ldbc.Person_hasInterest_Tag VALUES
(1262304000000, 1, 1), (1262304000000, 1, 2),
(1262390400000, 2, 1), (1262390400000, 2, 3),
(1262476800000, 3, 4), (1262476800000, 3, 5),
(1262563200000, 4, 2), (1262563200000, 4, 4),
(1262649600000, 5, 5);

-- Schema YAML declares WORK_AT.to_id = CompanyId and STUDY_AT.to_id = UniversityId
-- (matches LDBC SF CSV layout). Original mini_dataset.sql used `OrganisationId`
-- which diverges; renamed here so the canonical schema mappings apply unchanged.
CREATE OR REPLACE TABLE ldbc.Person_workAt_Organisation (
    creationDate BIGINT, PersonId BIGINT, CompanyId BIGINT, workFrom INT
) USING DELTA;

INSERT INTO ldbc.Person_workAt_Organisation VALUES
(1262304000000, 1, 3, 2015), (1262390400000, 2, 4, 2018), (1262649600000, 5, 3, 2020);

CREATE OR REPLACE TABLE ldbc.Person_studyAt_Organisation (
    creationDate BIGINT, PersonId BIGINT, UniversityId BIGINT, classYear INT
) USING DELTA;

INSERT INTO ldbc.Person_studyAt_Organisation VALUES
(1262304000000, 1, 1, 2010), (1262390400000, 2, 2, 2012), (1262476800000, 3, 2, 2014);

CREATE OR REPLACE TABLE ldbc.Person_knows_Person (
    creationDate BIGINT, Person1Id BIGINT, Person2Id BIGINT
) USING DELTA;

INSERT INTO ldbc.Person_knows_Person VALUES
(1262304000000, 1, 2), (1262304000000, 2, 1),
(1262390400000, 1, 3), (1262390400000, 3, 1),
(1262476800000, 2, 3), (1262476800000, 3, 2),
(1262563200000, 3, 4), (1262563200000, 4, 3),
(1262649600000, 4, 5), (1262649600000, 5, 4);

CREATE OR REPLACE TABLE ldbc.Forum (
    creationDate BIGINT, id BIGINT, title STRING
) USING DELTA;

INSERT INTO ldbc.Forum VALUES
(1275350400000, 1, 'Wall of Alice'),
(1275436800000, 2, 'Wall of Bob'),
(1275523200000, 3, 'Tech Discussion');

CREATE OR REPLACE TABLE ldbc.Forum_hasModerator_Person (
    creationDate BIGINT, ForumId BIGINT, PersonId BIGINT
) USING DELTA;

INSERT INTO ldbc.Forum_hasModerator_Person VALUES
(1275350400000, 1, 1), (1275436800000, 2, 2), (1275523200000, 3, 3);

CREATE OR REPLACE TABLE ldbc.Forum_hasMember_Person (
    creationDate BIGINT, ForumId BIGINT, PersonId BIGINT
) USING DELTA;

INSERT INTO ldbc.Forum_hasMember_Person VALUES
(1275350400000, 1, 1), (1275350400000, 1, 2), (1275350400000, 1, 3),
(1275436800000, 2, 2), (1275436800000, 2, 4),
(1275523200000, 3, 1), (1275523200000, 3, 2), (1275523200000, 3, 3),
(1275523200000, 3, 4), (1275523200000, 3, 5);

CREATE OR REPLACE TABLE ldbc.Forum_hasTag_Tag (
    creationDate BIGINT, ForumId BIGINT, TagId BIGINT
) USING DELTA;

INSERT INTO ldbc.Forum_hasTag_Tag VALUES
(1275523200000, 3, 1), (1275523200000, 3, 2), (1275523200000, 3, 3);

CREATE OR REPLACE TABLE ldbc.Post (
    creationDate BIGINT, id BIGINT, imageFile STRING, locationIP STRING,
    browserUsed STRING, language STRING, content STRING, length INT
) USING DELTA;

INSERT INTO ldbc.Post VALUES
(1275350400000, 101, '', '1.1.1.1', 'Chrome', 'en', 'Hello from Alice', 16),
(1275436800000, 102, '', '2.2.2.2', 'Firefox', 'en', 'Bob writes about databases', 26),
(1276041600000, 103, '', '1.1.1.1', 'Chrome', 'en', 'Alice on graphs', 15),
(1276128000000, 104, 'photo.jpg', '3.3.3.3', 'Safari', 'de', '', 0),
(1270000000000, 105, '', '2.2.2.2', 'Firefox', 'en', 'Old post by Bob', 15),
(1278000000000, 106, '', '4.4.4.4', 'Chrome', 'en', 'Dave on Rust', 12);

CREATE OR REPLACE TABLE ldbc.Post_hasCreator_Person (
    creationDate BIGINT, PostId BIGINT, PersonId BIGINT
) USING DELTA;

INSERT INTO ldbc.Post_hasCreator_Person VALUES
(1275350400000, 101, 1), (1275436800000, 102, 2), (1276041600000, 103, 1),
(1276128000000, 104, 3), (1270000000000, 105, 2), (1278000000000, 106, 4);

CREATE OR REPLACE TABLE ldbc.Post_isLocatedIn_Place (
    creationDate BIGINT, PostId BIGINT, PlaceId BIGINT
) USING DELTA;

INSERT INTO ldbc.Post_isLocatedIn_Place VALUES
(1275350400000, 101, 3), (1275436800000, 102, 4), (1276041600000, 103, 3),
(1276128000000, 104, 4), (1270000000000, 105, 4), (1278000000000, 106, 3);

CREATE OR REPLACE TABLE ldbc.Post_hasTag_Tag (
    creationDate BIGINT, PostId BIGINT, TagId BIGINT
) USING DELTA;

INSERT INTO ldbc.Post_hasTag_Tag VALUES
(1275350400000, 101, 1), (1275436800000, 102, 1), (1275436800000, 102, 3),
(1276041600000, 103, 2), (1276128000000, 104, 5), (1270000000000, 105, 1),
(1278000000000, 106, 4);

CREATE OR REPLACE TABLE ldbc.Forum_containerOf_Post (
    creationDate BIGINT, ForumId BIGINT, PostId BIGINT
) USING DELTA;

INSERT INTO ldbc.Forum_containerOf_Post VALUES
(1275350400000, 1, 101), (1276041600000, 1, 103),
(1275436800000, 2, 102), (1270000000000, 2, 105),
(1276128000000, 3, 104), (1278000000000, 3, 106);

CREATE OR REPLACE TABLE ldbc.Comment (
    creationDate BIGINT, id BIGINT, locationIP STRING, browserUsed STRING,
    content STRING, length INT
) USING DELTA;

INSERT INTO ldbc.Comment VALUES
(1275523200000, 201, '2.2.2.2', 'Firefox', 'Great post Alice!', 17),
(1275609600000, 202, '3.3.3.3', 'Safari', 'I agree with Bob', 16),
(1275696000000, 203, '4.4.4.4', 'Chrome', 'Interesting discussion', 21),
(1276214400000, 204, '1.1.1.1', 'Chrome', 'Nice photo Carol', 16),
(1276300800000, 205, '5.5.5.5', 'Firefox', 'Love this topic', 15);

CREATE OR REPLACE TABLE ldbc.Comment_hasCreator_Person (
    creationDate BIGINT, CommentId BIGINT, PersonId BIGINT
) USING DELTA;

INSERT INTO ldbc.Comment_hasCreator_Person VALUES
(1275523200000, 201, 2), (1275609600000, 202, 3), (1275696000000, 203, 4),
(1276214400000, 204, 1), (1276300800000, 205, 5);

CREATE OR REPLACE TABLE ldbc.Comment_isLocatedIn_Place (
    creationDate BIGINT, CommentId BIGINT, PlaceId BIGINT
) USING DELTA;

INSERT INTO ldbc.Comment_isLocatedIn_Place VALUES
(1275523200000, 201, 4), (1275609600000, 202, 4), (1275696000000, 203, 3),
(1276214400000, 204, 3), (1276300800000, 205, 6);

CREATE OR REPLACE TABLE ldbc.Comment_hasTag_Tag (
    creationDate BIGINT, CommentId BIGINT, TagId BIGINT
) USING DELTA;

INSERT INTO ldbc.Comment_hasTag_Tag VALUES
(1275523200000, 201, 1), (1275609600000, 202, 1), (1275696000000, 203, 2),
(1276214400000, 204, 5), (1276300800000, 205, 4);

CREATE OR REPLACE TABLE ldbc.Comment_replyOf_Post (
    creationDate BIGINT, CommentId BIGINT, PostId BIGINT
) USING DELTA;

INSERT INTO ldbc.Comment_replyOf_Post VALUES
(1275523200000, 201, 101), (1275609600000, 202, 102), (1275696000000, 203, 101),
(1276214400000, 204, 104), (1276300800000, 205, 106);

CREATE OR REPLACE TABLE ldbc.Comment_replyOf_Comment (
    creationDate BIGINT, Comment1Id BIGINT, Comment2Id BIGINT
) USING DELTA;

INSERT INTO ldbc.Comment_replyOf_Comment VALUES
(1275696000000, 203, 201);

CREATE OR REPLACE TABLE ldbc.Person_likes_Post (
    creationDate BIGINT, PersonId BIGINT, PostId BIGINT
) USING DELTA;

INSERT INTO ldbc.Person_likes_Post VALUES
(1275400000000, 2, 101), (1275500000000, 3, 101), (1275600000000, 1, 102),
(1276200000000, 4, 103), (1276300000000, 5, 106);

CREATE OR REPLACE TABLE ldbc.Person_likes_Comment (
    creationDate BIGINT, PersonId BIGINT, CommentId BIGINT
) USING DELTA;

INSERT INTO ldbc.Person_likes_Comment VALUES
(1275600000000, 1, 201), (1275700000000, 2, 202), (1276400000000, 3, 204);

-- Views for unified Message type
CREATE OR REPLACE VIEW ldbc.Message AS
SELECT creationDate, id, locationIP, browserUsed, content, length, imageFile, language, 'Post' AS type
FROM ldbc.Post
UNION ALL
SELECT creationDate, id, locationIP, browserUsed, content, length, '' AS imageFile, '' AS language, 'Comment' AS type
FROM ldbc.Comment;

CREATE OR REPLACE VIEW ldbc.Message_hasCreator_Person AS
SELECT creationDate, PostId AS MessageId, PersonId FROM ldbc.Post_hasCreator_Person
UNION ALL
SELECT creationDate, CommentId AS MessageId, PersonId FROM ldbc.Comment_hasCreator_Person;

CREATE OR REPLACE VIEW ldbc.Person_likes_Message AS
SELECT creationDate, PersonId, PostId AS MessageId FROM ldbc.Person_likes_Post
UNION ALL
SELECT creationDate, PersonId, CommentId AS MessageId FROM ldbc.Person_likes_Comment;

CREATE OR REPLACE VIEW ldbc.Comment_replyOf_Message AS
SELECT creationDate, CommentId, PostId AS MessageId FROM ldbc.Comment_replyOf_Post
UNION ALL
SELECT creationDate, Comment1Id AS CommentId, Comment2Id AS MessageId FROM ldbc.Comment_replyOf_Comment;

CREATE OR REPLACE VIEW ldbc.Message_replyOf_Message AS
SELECT CommentId AS MessageId, PostId AS TargetMessageId, creationDate FROM ldbc.Comment_replyOf_Post
UNION ALL
SELECT Comment1Id AS MessageId, Comment2Id AS TargetMessageId, creationDate FROM ldbc.Comment_replyOf_Comment;

CREATE OR REPLACE VIEW ldbc.Message_hasTag_Tag AS
SELECT creationDate, PostId AS MessageId, TagId FROM ldbc.Post_hasTag_Tag
UNION ALL
SELECT creationDate, CommentId AS MessageId, TagId FROM ldbc.Comment_hasTag_Tag;

-- Expose the location id as CountryId to match the YAML's Message IS_LOCATED_IN
-- Country edge (to_id: CountryId); base tables carry the column as PlaceId. Without
-- this, LDBC complex-3 generates `t5.CountryId` that fails to resolve (#399).
CREATE OR REPLACE VIEW ldbc.Message_isLocatedIn_Place AS
SELECT creationDate, PostId AS MessageId, PlaceId AS CountryId FROM ldbc.Post_isLocatedIn_Place
UNION ALL
SELECT creationDate, CommentId AS MessageId, PlaceId AS CountryId FROM ldbc.Comment_isLocatedIn_Place;
