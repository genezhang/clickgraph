-- LDBC Query: BI-1a
-- Status: PASS
-- Generated: 2025-12-21T09:15:27.771940
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (message:Post)
-- WHERE message.creationDate < 1325376000000
-- RETURN 
--     message.creationDate AS creationDate,
--     count(*) AS messageCount
-- ORDER BY messageCount DESC
-- LIMIT 20

-- Generated ClickHouse SQL:
SELECT 
      message.creationDate AS "creationDate", 
      count(*) AS "messageCount"
FROM ldbc.Post AS message
WHERE message.creationDate < 1325376000000
GROUP BY message.creationDate
ORDER BY messageCount DESC
LIMIT  20
