-- LDBC Query: COMPLEX-2
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.807193
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (person1:Person)-[:HAS_INTEREST]->(tag:Tag)<-[:HAS_INTEREST]-(person2:Person)
-- WHERE person1.id < person2.id
-- WITH person1, person2, count(tag) AS sharedInterests
-- WHERE sharedInterests >= 3
-- RETURN 
--     person1.id AS person1Id,
--     person2.id AS person2Id,
--     sharedInterests
-- ORDER BY sharedInterests DESC
-- LIMIT 50

-- Generated ClickHouse SQL:
WITH with_person1_person2_sharedInterests_cte_1 AS (SELECT 
      anyLast(person1.birthday) AS "person1_birthday", 
      anyLast(person1.browserUsed) AS "person1_browserUsed", 
      anyLast(person1.creationDate) AS "person1_creationDate", 
      anyLast(person1.firstName) AS "person1_firstName", 
      anyLast(person1.gender) AS "person1_gender", 
      person1.id AS "person1_id", 
      anyLast(person1.lastName) AS "person1_lastName", 
      anyLast(person1.locationIP) AS "person1_locationIP", 
      anyLast(person2.birthday) AS "person2_birthday", 
      anyLast(person2.browserUsed) AS "person2_browserUsed", 
      anyLast(person2.creationDate) AS "person2_creationDate", 
      anyLast(person2.firstName) AS "person2_firstName", 
      anyLast(person2.gender) AS "person2_gender", 
      person2.id AS "person2_id", 
      anyLast(person2.lastName) AS "person2_lastName", 
      anyLast(person2.locationIP) AS "person2_locationIP", 
      count(*) AS "sharedInterests"
FROM ldbc.Person AS person2
INNER JOIN ldbc.Person_hasInterest_Tag AS t66 ON t66.PersonId = person2.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t66.TagId
INNER JOIN ldbc.Person_hasInterest_Tag AS tag ON tag.PersonId = t66.TagId
INNER JOIN ldbc.Person_hasInterest_Tag AS t66 ON t66.PersonId = person1.id
GROUP BY person1.id, person2.id
HAVING sharedInterests >= 3
)
SELECT 
      person1_person2_sharedInterests.person1_id AS "person1Id", 
      person1_person2_sharedInterests.person2_id AS "person2Id", 
      sharedInterests AS "sharedInterests"
FROM with_person1_person2_sharedInterests_cte_1 AS person1_person2_sharedInterests
WHERE person1_person2_sharedInterests.person1_id < person1_person2_sharedInterests.person2_id
ORDER BY sharedInterests DESC
LIMIT  50
