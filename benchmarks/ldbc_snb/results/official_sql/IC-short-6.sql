-- LDBC Official Query: IC-short-6
-- Status: PASS
-- Generated: 2026-01-09T17:20:49.227695
-- Database: ldbc_snb

-- Original Cypher Query:
-- MATCH (m:Message {id: $messageId })-[:REPLY_OF*0..]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person)
-- RETURN
--     f.id AS forumId,
--     f.title AS forumTitle,
--     mod.id AS moderatorId,
--     mod.firstName AS moderatorFirstName,
--     mod.lastName AS moderatorLastName

-- Generated ClickHouse SQL:
WITH RECURSIVE vlp_cte13 AS (
    SELECT 
        start_node.id as start_id,
        start_node.id as end_id,
        0 as hop_count,
        CAST([] AS Array(Tuple(UInt64, UInt64))) as path_edges,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.id] as path_nodes
    FROM ldbc.Message AS start_node
    WHERE start_node.id = $messageId
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.MessageId, rel.TargetMessageId)]) as path_edges,
        arrayConcat(vp.path_relationships, ['REPLY_OF']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte13 vp
    JOIN ldbc.Post AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.Message_replyOf_Message AS rel ON current_node.id = rel.MessageId
    JOIN ldbc.Post AS end_node ON rel.TargetMessageId = end_node.id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.MessageId, rel.TargetMessageId))
)
SELECT 
      f.id AS "forumId", 
      f.title AS "forumTitle", 
      mod.id AS "moderatorId", 
      mod.firstName AS "moderatorFirstName", 
      mod.lastName AS "moderatorLastName"
FROM vlp_cte13 AS vlp13
JOIN ldbc.Message AS m ON vlp13.start_id = m.id
JOIN ldbc.Post AS p ON vlp13.end_id = p.id
INNER JOIN ldbc.Forum_containerOf_Post AS t95 ON t95.PostId = p.id
INNER JOIN ldbc.Forum AS f ON f.id = t95.ForumId
INNER JOIN ldbc.Forum_hasModerator_Person AS t96 ON t96.ForumId = f.id
INNER JOIN ldbc.Person AS mod ON mod.id = t96.PersonId

SETTINGS max_recursive_cte_evaluation_depth = 100

