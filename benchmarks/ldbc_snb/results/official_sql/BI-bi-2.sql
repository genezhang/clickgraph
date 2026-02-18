-- LDBC Official Query: BI-bi-2
-- Status: PASS
-- Generated: 2026-02-17T19:11:55.642652
-- Database: ldbc_snb

-- Original Cypher Query:
-- MATCH (tag:Tag)-[:HAS_TYPE]->(:TagClass {name: $tagClass})
-- OPTIONAL MATCH (message1:Message)-[:HAS_TAG]->(tag)
--   WHERE $date <= message1.creationDate
--     AND message1.creationDate < $date + duration({days: 100})
-- WITH tag, count(message1) AS countWindow1
-- OPTIONAL MATCH (message2:Message)-[:HAS_TAG]->(tag)
--   WHERE $date + duration({days: 100}) <= message2.creationDate
--     AND message2.creationDate < $date + duration({days: 200})
-- WITH
--   tag,
--   countWindow1,
--   count(message2) AS countWindow2
-- RETURN
--   tag.name,
--   countWindow1,
--   countWindow2,
--   abs(countWindow1 - countWindow2) AS diff
-- ORDER BY
--   diff DESC,
--   tag.name ASC
-- LIMIT 100

-- Generated ClickHouse SQL:
WITH with_countWindow1_tag_cte_1 AS (SELECT 
      tag.id AS "p3_tag_id", 
      anyLast(tag.name) AS "p3_tag_name", 
      count(message1.id) AS "countWindow1"
FROM ldbc.Tag AS tag
INNER JOIN ldbc.Tag_hasType_TagClass AS t42 ON t42.TagId = tag.id
INNER JOIN ldbc.TagClass AS t41 ON t41.id = t42.TagClassId
LEFT JOIN ldbc.Message_hasTag_Tag AS t43 ON t43.TagId = tag.id
LEFT JOIN (SELECT * FROM ldbc.Message WHERE ($date <= creationDate AND creationDate < $date + toIntervalDay(100))) AS message1 ON message1.id = t43.MessageId
WHERE t41.name = $tagClass
GROUP BY tag.id
), 
with_countWindow1_countWindow2_tag_cte_1 AS (SELECT 
      countWindow1_tag.p3_tag_id AS "p3_tag_id", 
      anyLast(countWindow1_tag.p3_tag_name) AS "p3_tag_name", 
      anyLast(countWindow1_tag.countWindow1) AS "countWindow1", 
      count(message2.id) AS "countWindow2"
FROM ldbc.Message AS message2
LEFT JOIN ldbc.Message_hasTag_Tag AS t44 ON t44.MessageId = message2.id
LEFT JOIN with_countWindow1_tag_cte_1 AS countWindow1_tag ON countWindow1_tag.p3_tag_id = t44.TagId
GROUP BY countWindow1_tag.p3_tag_id
)
SELECT 
      countWindow1_countWindow2_tag.p3_tag_name AS "tag.name", 
      countWindow1_countWindow2_tag.countWindow1 AS "countWindow1", 
      countWindow1_countWindow2_tag.countWindow2 AS "countWindow2", 
      abs(countWindow1_countWindow2_tag.countWindow1 - countWindow1_countWindow2_tag.countWindow2) AS "diff"
FROM with_countWindow1_countWindow2_tag_cte_1 AS countWindow1_countWindow2_tag
ORDER BY diff DESC, countWindow1_countWindow2_tag.p3_tag_name ASC
LIMIT  100
