-- LDBC Query: BI-5
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.781822
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (post:Post)-[:HAS_TAG]->(tag:Tag)
-- MATCH (post)-[:HAS_CREATOR]->(person:Person)
-- OPTIONAL MATCH (post)<-[:LIKES]-(liker:Person)
-- WITH person, tag, count(DISTINCT post) AS postCount, count(DISTINCT liker) AS likeCount
-- RETURN 
--     person.id AS personId,
--     tag.name AS tagName,
--     postCount,
--     likeCount,
--     postCount + 10 * likeCount AS score
-- ORDER BY score DESC, personId
-- LIMIT 100

-- Generated ClickHouse SQL:
WITH with_likeCount_person_postCount_tag_cte_1 AS (SELECT 
      anyLast(person.birthday) AS "person_birthday", 
      anyLast(person.browserUsed) AS "person_browserUsed", 
      anyLast(person.creationDate) AS "person_creationDate", 
      anyLast(person.firstName) AS "person_firstName", 
      anyLast(person.gender) AS "person_gender", 
      person.id AS "person_id", 
      anyLast(person.lastName) AS "person_lastName", 
      anyLast(person.locationIP) AS "person_locationIP", 
      tag.id AS "tag_id", 
      anyLast(tag.name) AS "tag_name", 
      anyLast(tag.url) AS "tag_url", 
      count(DISTINCT post) AS "postCount", 
      count(DISTINCT liker) AS "likeCount"
FROM ldbc.Person AS liker
LEFT JOIN ldbc.Person_likes_Post AS t15 ON t15.PersonId = liker.id
LEFT JOIN ldbc.Post_hasCreator_Person AS post ON post.PostId = t15.PostId
INNER JOIN ldbc.Post_hasTag_Tag AS t13 ON t13.PostId = post.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t13.TagId
INNER JOIN ldbc.Post_hasCreator_Person AS t14 ON t14.PostId = post.id
INNER JOIN ldbc.Person AS person ON person.id = t14.PersonId
GROUP BY person.id, tag.id
)
SELECT 
      likeCount_person_postCount_tag.person_id AS "personId", 
      likeCount_person_postCount_tag.tag_name AS "tagName", 
      likeCount_person_postCount_tag.postCount AS "postCount", 
      likeCount_person_postCount_tag.likeCount AS "likeCount", 
      likeCount_person_postCount_tag.postCount + 10 * likeCount_person_postCount_tag.likeCount AS "score"
FROM with_likeCount_person_postCount_tag_cte_1 AS likeCount_person_postCount_tag
ORDER BY score DESC, personId ASC
LIMIT  100
