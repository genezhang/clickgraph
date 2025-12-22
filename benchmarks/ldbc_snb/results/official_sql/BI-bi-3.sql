-- LDBC Official Query: BI-bi-3
-- Status: PASS
-- Generated: 2025-12-21T09:22:44.082738
-- Database: ldbc

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
WITH RECURSIVE vlp_cte10 AS (
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
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.from_id, rel.to_id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['REPLY_OF']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_cte10 vp
    JOIN ldbc.Forum AS current_node ON vp.end_id = current_node.id
    JOIN ldbc.ERROR_SCHEMA_MISSING_REPLY_OF_FROM_Some("Message")_TO_Some("") AS rel ON current_node.id = rel.from_id
    JOIN ldbc.Forum AS end_node ON rel.to_id = end_node.id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id))
)
SELECT 
      forum.id AS "forum.id", 
      forum.title AS "forum.title", 
      forum.creationDate AS "forum.creationDate", 
      person.id AS "person.id", 
      count(DISTINCT message.id) AS "messageCount"
FROM vlp_cte10 AS vlp10
JOIN ldbc.Message AS message ON vlp10.start_id = message.id
JOIN ldbc.Forum AS post ON vlp10.end_id = post.id
INNER JOIN ldbc.Person_isLocatedIn_Place AS t161 ON t161.CityId = t157.id
INNER JOIN ldbc.Message_hasTag_Tag AS t165 ON t165.MessageId = start_node.id
INNER JOIN ldbc.Place_isPartOf_Place AS t160 ON t160.Place1Id = t157.id
INNER JOIN ldbc.Place AS t156 ON t156.id = t160.Place2Id
INNER JOIN ldbc.Person AS person ON person.id = t161.PersonId
INNER JOIN ldbc.Forum_hasModerator_Person AS t162 ON t161.PersonId = t162.PersonId
INNER JOIN ldbc.Tag AS t158 ON t158.id = t165.TagId
INNER JOIN ldbc.Tag_hasType_TagClass AS t166 ON t165.TagId = t166.TagId
INNER JOIN ldbc.Forum_containerOf_Post AS t163 ON t162.ForumId = t163.ForumId
INNER JOIN ldbc.Forum AS forum ON forum.id = t162.ForumId
INNER JOIN ldbc.TagClass AS t159 ON t159.id = t166.TagClassId
INNER JOIN ldbc.Post AS post ON end_node.id = t163.PostId
WHERE (((t159.name = $tagClass AND t156.name = $country) AND t157.type = 'City') AND t156.type = 'Country')
GROUP BY forum.id, forum.title, forum.creationDate, person.id
ORDER BY messageCount DESC, forum.id ASC
LIMIT  20
SETTINGS max_recursive_cte_evaluation_depth = 100

