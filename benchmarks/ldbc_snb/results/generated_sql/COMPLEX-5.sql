-- LDBC Query: COMPLEX-5
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.810915
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (company:Company)<-[:WORK_AT]-(employee:Person)
-- MATCH (employee)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag)
-- RETURN 
--     company.name AS companyName,
--     tag.name AS topicTag,
--     count(post) AS postCount
-- ORDER BY postCount DESC
-- LIMIT 50

-- Generated ClickHouse SQL:
SELECT 
      company.name AS "companyName", 
      tag.name AS "topicTag", 
      count(post.id) AS "postCount"
FROM ldbc.Person AS employee
INNER JOIN ldbc.Person_workAt_Organisation AS t106 ON t106.PersonId = employee.id
INNER JOIN ldbc.Post_hasCreator_Person AS t107 ON t106.PersonId = t107.PersonId
INNER JOIN ldbc.Post_hasTag_Tag AS t108 ON t107.PostId = t108.PostId
INNER JOIN ldbc.Post_hasCreator_Person AS t107 ON t107.PersonId = employee.id
INNER JOIN ldbc.Post AS post ON post.id = t107.PostId
INNER JOIN ldbc.Tag AS tag ON tag.id = t108.TagId
INNER JOIN ldbc.Organisation AS company ON company.id = t106.CompanyId
INNER JOIN ldbc.Post_hasTag_Tag AS t108 ON t108.PostId = post.id
WHERE company.type = 'Company'
GROUP BY company.name, tag.name
ORDER BY postCount DESC
LIMIT  50
