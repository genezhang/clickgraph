-- LDBC Official Query: BI-bi-1
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.023437
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (message:Message)
-- WHERE message.creationDate < $datetime
-- WITH count(message) AS totalMessageCountInt // this should be a subquery once Cypher supports it
-- WITH toFloat(totalMessageCountInt) AS totalMessageCount
-- MATCH (message:Message)
-- WHERE message.creationDate < $datetime
--   AND message.content IS NOT NULL
-- WITH
--   totalMessageCount,
--   message,
--   message.creationDate.year AS year
-- WITH
--   totalMessageCount,
--   year,
--   message:Comment AS isComment,
--   CASE
--     WHEN message.length <  40 THEN 0
--     WHEN message.length <  80 THEN 1
--     WHEN message.length < 160 THEN 2
--     ELSE                           3
--   END AS lengthCategory,
--   count(message) AS messageCount,
--   sum(message.length) / toFloat(count(message)) AS averageMessageLength,
--   sum(message.length) AS sumMessageLength
-- RETURN
--   year,
--   isComment,
--   lengthCategory,
--   messageCount,
--   averageMessageLength,
--   sumMessageLength,
--   messageCount / totalMessageCount AS percentageOfMessages
-- ORDER BY
--   year DESC,
--   isComment ASC,
--   lengthCategory ASC

-- Generated ClickHouse SQL:
WITH with_totalMessageCountInt_cte_1 AS (SELECT 
      count(*) AS "totalMessageCountInt"
FROM ldbc.Message AS message
WHERE message.creationDate < $datetime
)
SELECT 
      totalMessageCountInt.totalMessageCountInt AS "totalMessageCountInt.totalMessageCountInt"
FROM with_totalMessageCountInt_cte_1 AS totalMessageCountInt

