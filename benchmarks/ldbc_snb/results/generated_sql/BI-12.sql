-- LDBC Query: BI-12
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.792798
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (person:Person)
-- OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(post:Post)
-- WITH person, count(post) AS postCount
-- RETURN 
--     postCount AS messageCount,
--     count(person) AS personCount
-- ORDER BY personCount DESC, messageCount DESC

-- Generated ClickHouse SQL:
WITH with_person_postCount_cte_1 AS (SELECT 
      anyLast(person.birthday) AS "person_birthday", 
      anyLast(person.browserUsed) AS "person_browserUsed", 
      anyLast(person.creationDate) AS "person_creationDate", 
      anyLast(person.firstName) AS "person_firstName", 
      anyLast(person.gender) AS "person_gender", 
      person.id AS "person_id", 
      anyLast(person.lastName) AS "person_lastName", 
      anyLast(person.locationIP) AS "person_locationIP", 
      count(*) AS "postCount"
FROM ldbc.Post AS post
LEFT JOIN ldbc.Post_hasCreator_Person AS t35 ON t35.PostId = post.id
LEFT JOIN ldbc.Person AS person ON person.id = t35.PersonId
GROUP BY person.id
)
SELECT 
      person_postCount.postCount AS "messageCount", 
      count(person_postCount.person_id) AS "personCount"
FROM with_person_postCount_cte_1 AS person_postCount
GROUP BY postCount
ORDER BY personCount DESC, messageCount DESC

