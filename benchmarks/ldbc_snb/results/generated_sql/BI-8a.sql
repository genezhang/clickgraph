-- LDBC Query: BI-8a
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.785555
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (tag:Tag)<-[:HAS_INTEREST]-(person:Person)
-- RETURN 
--     tag.name AS tagName,
--     count(person) AS interestedCount
-- ORDER BY interestedCount DESC
-- LIMIT 50

-- Generated ClickHouse SQL:
SELECT 
      tag.name AS "tagName", 
      count(person.id) AS "interestedCount"
FROM ldbc.Person AS person
INNER JOIN ldbc.Person_hasInterest_Tag AS t22 ON t22.PersonId = person.id
INNER JOIN ldbc.Tag AS tag ON tag.id = t22.TagId
GROUP BY tag.name
ORDER BY interestedCount DESC
LIMIT  50
