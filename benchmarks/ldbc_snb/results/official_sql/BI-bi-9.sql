-- LDBC Official Query: BI-bi-9
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.106831
-- Database: ldbc

-- Original Cypher Query:
-- MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)<-[:REPLY_OF*0..]-(reply:Message)
-- WHERE  post.creationDate >= $startDate
--   AND  post.creationDate <= $endDate
--   AND reply.creationDate >= $startDate
--   AND reply.creationDate <= $endDate
-- RETURN
--   person.id,
--   person.firstName,
--   person.lastName,
--   count(DISTINCT post) AS threadCount,
--   count(DISTINCT reply) AS messageCount
-- ORDER BY
--   messageCount DESC,
--   person.id ASC
-- LIMIT 100

-- Generated ClickHouse SQL:
WITH RECURSIVE vlp_cte11 AS (
    SELECT 
        start_node.id as start_id,
        start_node.id as end_id,
        0 as hop_count,
        CAST([] AS Array(Tuple(UInt64, UInt64))) as path_edges,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.id] as path_nodes
    FROM ldbc.Message AS start_node
    WHERE (start_node.creationDate >= $startDate AND start_node.creationDate <= $endDate) AND (start_node.creationDate >= $startDate AND start_node.creationDate <= $endDate)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.from_id, rel.to_id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['REPLY_OF']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte11 vp
    JOIN ldbc.Post AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.ERROR_SCHEMA_MISSING_REPLY_OF_FROM_Some("Message")_TO_Some("") AS rel ON current_node.id = rel.from_id
    JOIN ldbc.Post AS end_node ON rel.to_id = end_node.id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id))
      AND (end_node.creationDate >= $startDate AND end_node.creationDate <= $endDate)
)
SELECT 
      person.id AS "person.id", 
      person.firstName AS "person.firstName", 
      person.lastName AS "person.lastName", 
      count(DISTINCT post.id) AS "threadCount", 
      count(DISTINCT reply.id) AS "messageCount"
FROM vlp_cte11 AS vlp11
JOIN ldbc.Message AS reply ON vlp11.start_id = reply.id
JOIN ldbc.Post AS post ON vlp11.end_id = post.id
INNER JOIN ldbc.Post_hasCreator_Person AS t182 ON t182.PostId = end_node.id
INNER JOIN ldbc.Person AS person ON person.id = t182.PersonId
WHERE (post.creationDate >= $startDate AND post.creationDate <= $endDate)
GROUP BY person.id, person.firstName, person.lastName
ORDER BY messageCount DESC, person.id ASC
LIMIT  100
SETTINGS max_recursive_cte_evaluation_depth = 100

