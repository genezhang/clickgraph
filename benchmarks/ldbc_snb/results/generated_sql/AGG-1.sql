-- LDBC Query: AGG-1
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.800009
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (p:Person) RETURN 'Person' AS type, count(*) AS cnt
-- UNION ALL
-- MATCH (p:Post) RETURN 'Post' AS type, count(*) AS cnt
-- UNION ALL
-- MATCH (c:Comment) RETURN 'Comment' AS type, count(*) AS cnt
-- UNION ALL
-- MATCH (f:Forum) RETURN 'Forum' AS type, count(*) AS cnt
-- UNION ALL
-- MATCH (t:Tag) RETURN 'Tag' AS type, count(*) AS cnt

-- Generated ClickHouse SQL:
SELECT 
      "type" AS "type", 
      count(*) AS "cnt"
FROM (
SELECT 
      'Person' AS "type"
FROM ldbc.Person AS p
UNION ALL 
SELECT 
      'Post' AS "type"
FROM ldbc.Post AS p
UNION ALL 
SELECT 
      'Comment' AS "type"
FROM ldbc.Comment AS c
UNION ALL 
SELECT 
      'Forum' AS "type"
FROM ldbc.Forum AS f
UNION ALL 
SELECT 
      'Tag' AS "type"
FROM ldbc.Tag AS t
) AS __union
GROUP BY "type"

