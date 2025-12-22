-- LDBC Query: COMPLEX-4
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.809745
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (uni:Organisation)<-[:STUDY_AT]-(person:Person)
-- WHERE uni.type = 'University'
-- WITH uni, collect(person) AS alumni
-- WHERE size(alumni) > 1
-- UNWIND alumni AS p1
-- MATCH (p1)-[:KNOWS]-(p2:Person)
-- WHERE p2 IN alumni AND p1.id < p2.id
-- RETURN 
--     uni.name AS universityName,
--     count(*) AS alumniConnections
-- ORDER BY alumniConnections DESC
-- LIMIT 20

-- Generated ClickHouse SQL:
WITH with_alumni_uni_cte AS (SELECT 
      uni.id AS "uni_id", 
      anyLast(uni.name) AS "uni_name", 
      anyLast(uni.type) AS "uni_type", 
      anyLast(uni.url) AS "uni_url", 
      groupArray(tuple(person.birthday, person.browserUsed, person.creationDate, person.firstName, person.gender, person.id, person.lastName, person.locationIP)) AS "alumni"
FROM ldbc.Person AS person
INNER JOIN STUDY_AT AS t70 ON t70.from_id = person.id
INNER JOIN ldbc.Organisation AS uni ON uni.id = t70.to_id
GROUP BY uni.id
), 
with_alumni_uni_cte AS (SELECT 
      uni.id AS "uni_id", 
      anyLast(uni.name) AS "uni_name", 
      anyLast(uni.type) AS "uni_type", 
      anyLast(uni.url) AS "uni_url", 
      groupArray(tuple(person.birthday, person.browserUsed, person.creationDate, person.firstName, person.gender, person.id, person.lastName, person.locationIP)) AS "alumni"
FROM ldbc.Person AS person
INNER JOIN STUDY_AT AS t70 ON t70.from_id = person.id
INNER JOIN ldbc.Organisation AS uni ON uni.id = t70.to_id
GROUP BY uni.id
)
SELECT 
      "universityName" AS "universityName", 
      count(*) AS "alumniConnections"
FROM (
SELECT 
      uni.name AS "universityName"
FROM ldbc.Person_knows_Person AS t71
INNER JOIN ldbc.Person AS p2 ON p2.id = t71.Person2Id
WHERE (p2 IN alumni AND p1.id < p2.id)
UNION ALL 
SELECT 
      uni.name AS "universityName"
FROM ldbc.Person AS p2
INNER JOIN ldbc.Person_knows_Person AS t71 ON t71.Person1Id = p2.id
INNER JOIN ldbc.Person AS p1 ON p1.id = t71.Person2Id
WHERE (p2 IN alumni AND p1.id < p2.id)
) AS __union
GROUP BY "universityName"
ORDER BY "alumniConnections" DESC
LIMIT  20
