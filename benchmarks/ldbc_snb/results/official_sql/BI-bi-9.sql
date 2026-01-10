-- LDBC Official Query: BI-bi-9
-- Status: PASS
-- Generated: 2026-01-09T17:20:49.184095
-- Database: ldbc_snb

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
WITH RECURSIVE vlp_cte3 AS (
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
        end_node.TargetMessageId as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.MessageId, rel.TargetMessageId)]) as path_edges,
        arrayConcat(vp.path_relationships, ['REPLY_OF']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.TargetMessageId]) as path_nodes
    FROM vlp_cte3 vp
    JOIN ldbc.Post AS current_node ON vp.end_id = current_node.TargetMessageId
    JOIN ldbc.Message_replyOf_Message AS rel ON current_node.TargetMessageId = rel.MessageId
    JOIN ldbc.Post AS end_node ON rel.TargetMessageId = end_node.TargetMessageId
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.MessageId, rel.TargetMessageId))
      AND (end_node.creationDate >= $startDate AND end_node.creationDate <= $endDate)
)
SELECT 
      person.id AS "person.id", 
      person.firstName AS "person.firstName", 
      person.lastName AS "person.lastName", 
      count(DISTINCT post.id) AS "threadCount", 
      count(DISTINCT reply.id) AS "messageCount"
FROM vlp_cte3 AS vlp3
JOIN ldbc.Message AS reply ON vlp3.start_id = reply.id
JOIN ldbc.Post AS post ON vlp3.end_id = post.TargetMessageId
INNER JOIN ldbc.Post_hasCreator_Person AS t58 ON t58.PostId = post.id
INNER JOIN ldbc.Person AS person ON person.id = t58.PersonId
WHERE (post.creationDate >= $startDate AND post.creationDate <= $endDate)
GROUP BY person.id, person.firstName, person.lastName
ORDER BY messageCount DESC, person.id ASC
LIMIT  100
SETTINGS max_recursive_cte_evaluation_depth = 100

