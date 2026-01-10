-- LDBC Official Query: BI-bi-3
-- Status: PASS
-- Generated: 2026-01-09T17:20:49.171936
-- Database: ldbc_snb

-- Original Cypher Query:
-- MATCH
--   (:Country {name: $country})<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-
--   (person:Person)<-[:HAS_MODERATOR]-(forum:Forum)-[:CONTAINER_OF]->
--   (post:Post)<-[:REPLY_OF*0..]-(message:Message)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass {name: $tagClass})
-- RETURN
--   forum.id,
--   forum.title,
--   forum.creationDate,
--   person.id,
--   count(DISTINCT message) AS messageCount
-- ORDER BY
--   messageCount DESC,
--   forum.id ASC
-- LIMIT 20

-- Generated ClickHouse SQL:
WITH RECURSIVE vlp_cte2 AS (
    SELECT 
        start_node.id as start_id,
        start_node.id as end_id,
        0 as hop_count,
        CAST([] AS Array(Tuple(UInt64, UInt64))) as path_edges,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.id] as path_nodes
    FROM ldbc.Message AS start_node
    UNION ALL
    SELECT
        vp.start_id,
        end_node.TargetMessageId as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.MessageId, rel.TargetMessageId)]) as path_edges,
        arrayConcat(vp.path_relationships, ['REPLY_OF']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.TargetMessageId]) as path_nodes
    FROM vlp_cte2 vp
    JOIN ldbc.Forum AS current_node ON vp.end_id = current_node.TargetMessageId
    JOIN ldbc.Message_replyOf_Message AS rel ON current_node.TargetMessageId = rel.MessageId
    JOIN ldbc.Forum AS end_node ON rel.TargetMessageId = end_node.TargetMessageId
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.MessageId, rel.TargetMessageId))
)
SELECT 
      forum.id AS "forum.id", 
      forum.title AS "forum.title", 
      forum.creationDate AS "forum.creationDate", 
      person.id AS "person.id", 
      count(DISTINCT message.id) AS "messageCount"
FROM vlp_cte2 AS vlp2
JOIN ldbc.Message AS message ON vlp2.start_id = message.id
JOIN ldbc.Forum AS post ON vlp2.end_id = post.TargetMessageId
INNER JOIN ldbc.Message_hasTag_Tag AS t45 ON t45.MessageId = message.id
INNER JOIN ldbc.Tag AS t38 ON t38.id = t45.TagId
INNER JOIN ldbc.Tag_hasType_TagClass AS t46 ON t46.TagId = t38.id
INNER JOIN ldbc.Message_replyOf_Message AS t44 ON t44.MessageId = t45.MessageId
INNER JOIN ldbc.TagClass AS t39 ON t39.id = t46.TagClassId
INNER JOIN ldbc.Forum_hasModerator_Person AS t42 ON t42.PersonId = person.id
INNER JOIN ldbc.Person_isLocatedIn_Place AS t41 ON t41.CityId = t37.id
INNER JOIN ldbc.Forum_containerOf_Post AS t43 ON t43.ForumId = forum.id
INNER JOIN ldbc.Person AS person ON person.id = t41.PersonId
INNER JOIN ldbc.Post AS post ON post.id = t43.PostId
INNER JOIN ldbc.Forum AS forum ON forum.id = t42.ForumId
WHERE (((t39.name = $tagClass AND t36.name = $country) AND (t37.type = 'City')) AND (t36.type = 'Country'))
GROUP BY forum.id, forum.title, forum.creationDate, person.id
ORDER BY messageCount DESC, forum.id ASC
LIMIT  20
SETTINGS max_recursive_cte_evaluation_depth = 100

