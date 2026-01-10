-- LDBC Official Query: IC-short-4
-- Status: PASS
-- Generated: 2026-01-09T17:20:49.223846
-- Database: ldbc_snb

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

