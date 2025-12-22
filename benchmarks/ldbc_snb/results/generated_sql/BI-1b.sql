-- LDBC Query: BI-1b
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.773216
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (comment:Comment)
-- RETURN count(*) AS totalComments

-- Generated ClickHouse SQL:
SELECT 
      count(*) AS "totalComments"
FROM ldbc.Comment AS comment

