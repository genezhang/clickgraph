-- LDBC Official Query: IC-short-4
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.184340
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (m:Message {id: $messageId })
-- RETURN
--     m.creationDate as messageCreationDate,
--     coalesce(m.content, m.imageFile) as messageContent

-- Generated ClickHouse SQL:
SELECT 
      m.creationDate AS "messageCreationDate", 
      coalesce(m.content, m.imageFile) AS "messageContent"
FROM ldbc.Message AS m
WHERE m.id = $messageId

