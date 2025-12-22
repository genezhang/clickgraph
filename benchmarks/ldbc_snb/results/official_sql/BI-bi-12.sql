-- LDBC Official Query: BI-bi-12
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.043950
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (person:Person)
-- OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(message:Message)-[:REPLY_OF*0..]->(post:Post)
-- WHERE message.content IS NOT NULL
--   AND message.length < $lengthThreshold
--   AND message.creationDate > $startDate
--   AND post.language IN $languages
-- WITH
--   person,
--   count(message) AS messageCount
-- RETURN
--   messageCount,
--   count(person) AS personCount
-- ORDER BY
--   personCount DESC,
--   messageCount DESC

-- Generated ClickHouse SQL:
WITH RECURSIVE vlp_cte9 AS (
    SELECT 
        start_node.id as start_id,
        start_node.id as end_id,
        0 as hop_count,
        CAST([] AS Array(Tuple(UInt64, UInt64))) as path_edges,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.id] as path_nodes
    FROM ldbc.Message AS start_node
    WHERE start_node.language IN $languages
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.from_id, rel.to_id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['REPLY_OF']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte9 vp
    JOIN ldbc.Post AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.ERROR_SCHEMA_MISSING_REPLY_OF_FROM_Some("")_TO_Some("Post") AS rel ON current_node.id = rel.from_id
    JOIN ldbc.Post AS end_node ON rel.to_id = end_node.id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id))
      AND end_node.language IN $languages
), 
with_messageCount_person_cte_1 AS (SELECT 
      anyLast(person.birthday) AS "person_birthday", 
      anyLast(person.browserUsed) AS "person_browserUsed", 
      anyLast(person.creationDate) AS "person_creationDate", 
      anyLast(person.firstName) AS "person_firstName", 
      anyLast(person.gender) AS "person_gender", 
      person.id AS "person_id", 
      anyLast(person.lastName) AS "person_lastName", 
      anyLast(person.locationIP) AS "person_locationIP", 
      count(*) AS "messageCount"
FROM ldbc.Message AS message
LEFT JOIN vlp_cte9 AS vlp9 ON vlp9.start_id = message.id
LEFT JOIN ldbc.Post AS post ON vlp9.end_id = post.id
WHERE (post.language IN $languages AND post.language IN $languages)
GROUP BY person.id
)
SELECT 
      messageCount_person.messageCount AS "messageCount", 
      count(messageCount_person.person_id) AS "personCount"
FROM with_messageCount_person_cte_1 AS messageCount_person
GROUP BY messageCount
ORDER BY personCount DESC, messageCount DESC

SETTINGS max_recursive_cte_evaluation_depth = 100

